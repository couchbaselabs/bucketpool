// Package purge empties a Couchbase Server bucket in place.  It replays every vbucket over
// a one-shot DCP feed and purges every document the feed reports, then leaves the bucket,
// its scopes, its collections and its indexes untouched.
//
// A DCP feed is the only way to see a tombstone that still carries xattrs, and neither a
// query nor a key/value read reports one.  A test harness can therefore reuse a bucket
// between tests instead of dropping and recreating it, which is far slower.
package purge

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

// The server rejects a multi-mutation that carries more than 16 operations.
const maxSubdocOps = 16

// xattrTableOfContents is the virtual xattr that lists the names of every xattr a document
// carries.  It reads back the names when the feed did not report them.
const xattrTableOfContents = "$XTOC"

// readyTimeout is how long to wait for a connection when the caller set no deadline.
const readyTimeout = 30 * time.Second

// bodyPath is the sub-document path of the document body itself.
const bodyPath = ""

// Options names the cluster and the bucket to empty.
type Options struct {
	ConnectionString string // the couchbase:// connection string of the cluster
	ManagementURL    string // the http:// management URL of one node
	Username         string // the administrator username
	Password         string // the administrator password
	Bucket           string // the bucket to empty
}

// Summary counts what one purge did.
type Summary struct {
	Processed int `json:"processed"`
	Purged    int `json:"purged"`
}

// collectionRef names a collection that a DCP collection ID refers to.
type collectionRef struct {
	scope      string
	collection string
}

// docKey identifies one document seen on the feed.
type docKey struct {
	collectionID uint32
	id           string
}

// docEvent is the latest feed event for a document.
type docEvent struct {
	seqNo   uint64
	deleted bool
	xattrs  []string
	// xattrsKnown is false when the event carried no value, so the purge has to read the
	// names of the xattrs back from the server.
	xattrsKnown bool
}

// docState is what a document carries right now, as read back from the server.
type docState struct {
	found   bool
	deleted bool
	xattrs  []string
}

// Run empties the bucket named in opts and reports what it did.  The caller sets how long
// the feed and the purge together are allowed to take, through the context.
//
// A nil Summary means the purge never started, so there is nothing to report.  A Summary
// alongside an error means the purge ran and some documents failed.
func Run(ctx context.Context, opts Options) (*Summary, error) {
	events, err := collectEvents(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("reading DCP feed: %w", err)
	}

	// The manifest is read after the feed, because a collection created while the tool runs
	// can then only add an entry.  The other order leaves the feed reporting documents in a
	// collection the map does not name.
	collections, err := fetchCollections(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("reading collection manifest: %w", err)
	}

	cluster, err := gocb.Connect(opts.ConnectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{Username: opts.Username, Password: opts.Password},
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", opts.ConnectionString, err)
	}
	defer func() { _ = cluster.Close(nil) }()

	bucket := cluster.Bucket(opts.Bucket)
	wait := time.Until(deadline(ctx))
	if err := bucket.WaitUntilReady(wait, &gocb.WaitUntilReadyOptions{Context: ctx}); err != nil {
		return nil, fmt.Errorf("waiting for bucket %q: %w", opts.Bucket, err)
	}

	purged, purgeErr := purgeAll(ctx, bucket, collections, events)
	return &Summary{Processed: len(events), Purged: purged}, purgeErr
}

// deadline returns the deadline of ctx, or one readyTimeout from now when ctx has none.
func deadline(ctx context.Context) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return time.Now().Add(readyTimeout)
}

// fetchCollections maps every collection ID in the bucket to its scope and collection name.
// gocb exposes the manifest by name only, so this reads the IDs the feed reports from REST.
func fetchCollections(ctx context.Context, opts Options) (map[uint32]collectionRef, error) {
	url := fmt.Sprintf("%s/pools/default/buckets/%s/scopes", strings.TrimSuffix(opts.ManagementURL, "/"), opts.Bucket)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth(opts.Username, opts.Password)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned %s", url, response.Status)
	}

	var manifest struct {
		Scopes []struct {
			Name        string `json:"name"`
			Collections []struct {
				Name string `json:"name"`
				UID  string `json:"uid"`
			} `json:"collections"`
		} `json:"scopes"`
	}
	if err := json.NewDecoder(response.Body).Decode(&manifest); err != nil {
		return nil, err
	}

	refs := make(map[uint32]collectionRef)
	for _, scope := range manifest.Scopes {
		for _, collection := range scope.Collections {
			id, err := strconv.ParseUint(collection.UID, 16, 32)
			if err != nil {
				return nil, fmt.Errorf("collection %s.%s has unparseable uid %q: %w",
					scope.Name, collection.Name, collection.UID, err)
			}
			refs[uint32(id)] = collectionRef{scope: scope.Name, collection: collection.Name}
		}
	}
	return refs, nil
}

// collectEvents runs a one-shot DCP feed over the whole bucket and returns the latest event
// for every document it saw, tombstones included.
func collectEvents(ctx context.Context, opts Options) (map[docKey]docEvent, error) {
	agent, err := newDCPAgent(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = agent.Close() }()

	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		return nil, fmt.Errorf("reading cluster config: %w", err)
	}
	numVbuckets, err := snapshot.NumVbuckets()
	if err != nil {
		return nil, fmt.Errorf("counting vbuckets: %w", err)
	}
	highSeqNos, err := highSeqNos(ctx, agent, snapshot, numVbuckets)
	if err != nil {
		return nil, err
	}

	obs := &observer{events: make(map[docKey]docEvent)}
	for vbID := range uint16(numVbuckets) {
		if highSeqNos[vbID] == 0 {
			continue
		}
		obs.streams.Add(1)
		if err := openStream(ctx, agent, obs, vbID, highSeqNos[vbID]); err != nil {
			obs.streams.Done()
			return nil, fmt.Errorf("opening stream for vbucket %d: %w", vbID, err)
		}
	}

	if err := obs.wait(ctx); err != nil {
		return nil, err
	}
	return obs.events, obs.err()
}

func newDCPAgent(ctx context.Context, opts Options) (*gocbcore.DCPAgent, error) {
	config := gocbcore.DCPAgentConfig{DefaultRetryStrategy: gocbcore.NewBestEffortRetryStrategy(nil)}
	if err := config.FromConnStr(opts.ConnectionString); err != nil {
		return nil, fmt.Errorf("parsing %q: %w", opts.ConnectionString, err)
	}
	config.BucketName = opts.Bucket
	config.UserAgent = "bucketpool"
	config.IoConfig.UseCollections = true
	config.SecurityConfig.Auth = gocbcore.PasswordAuthProvider{Username: opts.Username, Password: opts.Password}

	// IncludeXattrs is what makes the xattrs of a tombstone visible on the feed.
	flags := memd.DcpOpenFlagProducer | memd.DcpOpenFlagIncludeXattrs
	name := fmt.Sprintf("bucketpool-%d", time.Now().UnixNano())
	agent, err := gocbcore.CreateDcpAgent(&config, name, flags)
	if err != nil {
		return nil, fmt.Errorf("creating DCP agent: %w", err)
	}

	ready := make(chan error, 1)
	op, err := agent.WaitUntilReady(deadline(ctx), gocbcore.WaitUntilReadyOptions{},
		func(_ *gocbcore.WaitUntilReadyResult, err error) { ready <- err })
	if err == nil {
		err = awaitOp(ctx, op, ready)
	}
	if err != nil {
		_ = agent.Close()
		return nil, fmt.Errorf("waiting for DCP agent: %w", err)
	}
	return agent, nil
}

// awaitOp waits for a gocbcore callback, and cancels the operation when the context is done.
// The default retry strategy is best-effort, so an operation left alone retries for ever.
func awaitOp(ctx context.Context, op gocbcore.PendingOp, result chan error) error {
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		op.Cancel()
		<-result
		return ctx.Err()
	}
}

// highSeqNos returns the current sequence number of every active vbucket, which is where the
// one-shot streams stop.  Each node reports only its own active vbuckets, so every node is asked.
func highSeqNos(ctx context.Context, agent *gocbcore.DCPAgent, snapshot *gocbcore.ConfigSnapshot, numVbuckets int) ([]uint64, error) {
	numServers, err := snapshot.NumServers()
	if err != nil {
		return nil, fmt.Errorf("counting servers: %w", err)
	}

	seqNos := make([]uint64, numVbuckets)
	// Server indexes are 1-based here; 0 means "the node this agent is talking to".
	for serverIdx := 1; serverIdx <= numServers; serverIdx++ {
		result := make(chan error, 1)
		op, err := agent.GetVbucketSeqnos(serverIdx, memd.VbucketStateActive, gocbcore.GetVbucketSeqnoOptions{},
			func(entries []gocbcore.VbSeqNoEntry, err error) {
				for _, entry := range entries {
					if int(entry.VbID) < len(seqNos) && seqNos[entry.VbID] < uint64(entry.SeqNo) {
						seqNos[entry.VbID] = uint64(entry.SeqNo)
					}
				}
				result <- err
			})
		if err != nil {
			return nil, err
		}
		if err := awaitOp(ctx, op, result); err != nil {
			return nil, err
		}
	}
	return seqNos, nil
}

// openStream starts one vbucket at sequence 0 and ends it at endSeqNo.  A zero vbucket UUID
// asks the server to skip the failover-log check, which is what a from-scratch replay wants.
func openStream(ctx context.Context, agent *gocbcore.DCPAgent, obs *observer, vbID uint16, endSeqNo uint64) error {
	opened := make(chan error, 1)
	op, err := agent.OpenStream(vbID, memd.DcpStreamAddFlagActiveOnly, 0, 0,
		gocbcore.SeqNo(endSeqNo), 0, 0, obs, gocbcore.OpenStreamOptions{},
		func(_ []gocbcore.FailoverEntry, err error) { opened <- err })
	if err != nil {
		return err
	}
	return awaitOp(ctx, op, opened)
}

// purgeAll deletes every document the feed reported, along with all of its xattrs.
func purgeAll(ctx context.Context, bucket *gocb.Bucket, collections map[uint32]collectionRef, events map[docKey]docEvent) (int, error) {
	var purged int
	var errs []error
	for key, event := range events {
		// A deletion with no xattrs is an ordinary tombstone; the server reaps it on its own.
		if event.deleted && event.xattrsKnown && len(event.xattrs) == 0 {
			continue
		}
		ref, ok := collections[key.collectionID]
		if !ok {
			errs = append(errs, fmt.Errorf("document %q is in unknown collection %d", key.id, key.collectionID))
			continue
		}
		collection := bucket.Scope(ref.scope).Collection(ref.collection)
		if err := purgeDocument(ctx, collection, key.id, event); err != nil {
			errs = append(errs, fmt.Errorf("purging %q from %s.%s: %w", key.id, ref.scope, ref.collection, err))
			continue
		}
		purged++
	}
	return purged, errors.Join(errs...)
}

// purgeDocument removes the body of one document and every xattr it carries.  A full-document
// write keeps the system xattrs, the ones whose name starts with an underscore, so they have
// to be removed by name instead.
func purgeDocument(ctx context.Context, collection *gocb.Collection, id string, event docEvent) error {
	if !event.xattrsKnown {
		return purgeCurrent(ctx, collection, id)
	}
	err := removeContents(ctx, collection, id, event.xattrs, event.deleted)
	if !errors.Is(err, gocb.ErrPathNotFound) && !errors.Is(err, gocb.ErrDocumentNotFound) {
		return err
	}
	// The document changed between the feed and now: it became a tombstone, or one of the
	// xattrs went away.  Read back what it carries and purge that instead.
	return purgeCurrent(ctx, collection, id)
}

// purgeCurrent reads what the document carries now and removes it.
func purgeCurrent(ctx context.Context, collection *gocb.Collection, id string) error {
	state, err := readState(ctx, collection, id)
	if err != nil {
		return err
	}
	if !state.found {
		return nil
	}
	return removeContents(ctx, collection, id, state.xattrs, state.deleted)
}

// readState reports whether the document exists, whether it is a tombstone, and which xattrs
// it carries.  A tombstone answers only when AccessDeleted is set, so a live read comes first.
func readState(ctx context.Context, collection *gocb.Collection, id string) (docState, error) {
	names, err := lookupXattrNames(ctx, collection, id, false)
	if err == nil {
		return docState{found: true, xattrs: names}, nil
	}
	if !errors.Is(err, gocb.ErrDocumentNotFound) {
		return docState{}, err
	}

	names, err = lookupXattrNames(ctx, collection, id, true)
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return docState{}, nil
	}
	if err != nil {
		return docState{}, err
	}
	return docState{found: true, deleted: true, xattrs: names}, nil
}

// lookupXattrNames reads the names of the xattrs of one document from the virtual xattr $XTOC.
func lookupXattrNames(ctx context.Context, collection *gocb.Collection, id string, deleted bool) ([]string, error) {
	opts := &gocb.LookupInOptions{Context: ctx}
	if deleted {
		opts.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted
	}
	specs := []gocb.LookupInSpec{gocb.GetSpec(xattrTableOfContents, &gocb.GetSpecOptions{IsXattr: true})}
	result, err := collection.LookupIn(id, specs, opts)
	if err != nil {
		return nil, err
	}
	var names []string
	if err := result.ContentAt(0, &names); err != nil {
		if errors.Is(err, gocb.ErrPathNotFound) {
			// The document carries no xattrs at all.
			return nil, nil
		}
		return nil, fmt.Errorf("reading %s: %w", xattrTableOfContents, err)
	}
	return names, nil
}

// removeContents removes every named xattr, and the body as well when the document is not
// already a tombstone.  AccessDeleted is what lets a sub-document mutation reach a tombstone.
func removeContents(ctx context.Context, collection *gocb.Collection, id string, xattrs []string, deleted bool) error {
	if len(xattrs) == 0 {
		if deleted {
			// An ordinary tombstone; the server reaps it on its own.
			return nil
		}
		_, err := collection.Remove(id, &gocb.RemoveOptions{Context: ctx})
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil
		}
		return err
	}

	opts := &gocb.MutateInOptions{StoreSemantic: gocb.StoreSemanticsReplace, Context: ctx}
	if deleted {
		opts.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted
	}
	for _, paths := range removeBatches(xattrs, !deleted) {
		specs := make([]gocb.MutateInSpec, 0, len(paths))
		for _, path := range paths {
			if path == bodyPath {
				specs = append(specs, gocb.RemoveSpec(bodyPath, nil))
				continue
			}
			specs = append(specs, gocb.RemoveSpec(path, &gocb.RemoveSpecOptions{IsXattr: true}))
		}
		if _, err := collection.MutateIn(id, specs, opts); err != nil {
			return err
		}
	}
	return nil
}

// removeBatches splits the paths to remove into multi-mutations that stay inside the
// sub-document operation limit.  The body goes last, so that the xattrs of a live document go
// away in the same mutation that deletes it whenever they fit.
func removeBatches(xattrs []string, removeBody bool) [][]string {
	var batches [][]string
	var batch []string
	flush := func() {
		if len(batch) > 0 {
			batches = append(batches, batch)
			batch = nil
		}
	}
	for _, name := range xattrs {
		if len(batch) == maxSubdocOps {
			flush()
		}
		batch = append(batch, name)
	}
	if removeBody {
		if len(batch) == maxSubdocOps {
			flush()
		}
		batch = append(batch, bodyPath)
	}
	flush()
	return batches
}

// xattrNames returns the names of the xattrs a DCP value carries.  The value starts with the
// byte length of the xattr section, then one length-prefixed key\0value\0 pair per xattr.
//
// The lengths come off the wire as uint32, so every comparison here is done in uint64 to keep
// a hostile length from wrapping past a bounds check.
func xattrNames(value []byte) ([]string, error) {
	if len(value) < 4 {
		return nil, nil
	}
	sectionLen := uint64(binary.BigEndian.Uint32(value[0:4]))
	if sectionLen == 0 {
		return nil, nil
	}
	end := 4 + sectionLen
	if end > uint64(len(value)) {
		return nil, fmt.Errorf("xattr section claims %d bytes but the value holds %d", sectionLen, len(value)-4)
	}

	var names []string
	for pos := uint64(4); pos < end; {
		if pos+4 > end {
			return nil, errors.New("truncated xattr pair length")
		}
		pairLen := uint64(binary.BigEndian.Uint32(value[pos : pos+4]))
		pos += 4
		if pairLen == 0 || pos+pairLen > end {
			return nil, fmt.Errorf("invalid xattr pair length %d", pairLen)
		}
		name, _, found := strings.Cut(string(value[pos:pos+pairLen]), "\x00")
		if !found {
			return nil, errors.New("xattr pair is missing its name terminator")
		}
		names = append(names, name)
		pos += pairLen
	}
	return names, nil
}
