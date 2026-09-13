package purge

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

// observer implements gocbcore.StreamObserver.  It records the latest event per document and
// counts the open streams down so the caller knows when the one-shot feed is finished.
type observer struct {
	mu      sync.Mutex
	events  map[docKey]docEvent
	errs    []error
	streams sync.WaitGroup
}

// wait blocks until every stream has ended, or the context is done.
func (o *observer) wait(ctx context.Context) error {
	finished := make(chan struct{})
	go func() {
		o.streams.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("DCP feed did not finish: %w", ctx.Err())
	}
}

func (o *observer) err() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return errors.Join(o.errs...)
}

// record reads the xattr names out of the value of an event that carries one.
func (o *observer) record(key docKey, event docEvent, value []byte, datatype uint8) {
	if datatype&uint8(memd.DatatypeFlagXattrs) != 0 {
		names, err := xattrNames(value)
		if err != nil {
			o.mu.Lock()
			o.errs = append(o.errs, fmt.Errorf("decoding xattrs of %q: %w", key.id, err))
			o.mu.Unlock()
			return
		}
		event.xattrs = names
	}
	event.xattrsKnown = true
	o.store(key, event)
}

func (o *observer) store(key docKey, event docEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	// A document can appear in more than one snapshot, so the newest event wins.
	if previous, seen := o.events[key]; seen && previous.seqNo > event.seqNo {
		return
	}
	o.events[key] = event
}

func (o *observer) Mutation(mutation gocbcore.DcpMutation) {
	key := docKey{collectionID: mutation.CollectionID, id: string(mutation.Key)}
	o.record(key, docEvent{seqNo: mutation.SeqNo}, mutation.Value, mutation.Datatype)
}

func (o *observer) Deletion(deletion gocbcore.DcpDeletion) {
	key := docKey{collectionID: deletion.CollectionID, id: string(deletion.Key)}
	o.record(key, docEvent{seqNo: deletion.SeqNo, deleted: true}, deletion.Value, deletion.Datatype)
}

func (o *observer) Expiration(expiration gocbcore.DcpExpiration) {
	// An expiration carries no value, so whether the document kept any xattrs is unknown and
	// the purge has to read the names back from the server.
	key := docKey{collectionID: expiration.CollectionID, id: string(expiration.Key)}
	o.store(key, docEvent{seqNo: expiration.SeqNo, deleted: true})
}

func (o *observer) End(end gocbcore.DcpStreamEnd, err error) {
	if err != nil && !errors.Is(err, gocbcore.ErrDCPStreamClosed) {
		o.mu.Lock()
		o.errs = append(o.errs, fmt.Errorf("stream for vbucket %d ended: %w", end.VbID, err))
		o.mu.Unlock()
	}
	o.streams.Done()
}

// The remaining events describe the shape of the bucket rather than its data, and purging
// leaves that shape alone.

func (o *observer) SnapshotMarker(gocbcore.DcpSnapshotMarker)           {}
func (o *observer) CreateCollection(gocbcore.DcpCollectionCreation)     {}
func (o *observer) DeleteCollection(gocbcore.DcpCollectionDeletion)     {}
func (o *observer) FlushCollection(gocbcore.DcpCollectionFlush)         {}
func (o *observer) ModifyCollection(gocbcore.DcpCollectionModification) {}
func (o *observer) CreateScope(gocbcore.DcpScopeCreation)               {}
func (o *observer) DeleteScope(gocbcore.DcpScopeDeletion)               {}
func (o *observer) OSOSnapshot(gocbcore.DcpOSOSnapshot)                 {}
func (o *observer) SeqNoAdvanced(gocbcore.DcpSeqNoAdvanced)             {}
