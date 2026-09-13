package purge

import (
	"testing"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

func newObserver() *observer {
	return &observer{events: make(map[docKey]docEvent)}
}

func TestObserverRecordsXattrNames(t *testing.T) {
	obs := newObserver()
	value := encodeXattrs([][2]string{{"_sync", "{}"}, {"user", "1"}}, `{"a":1}`)
	obs.Mutation(gocbcore.DcpMutation{
		SeqNo:        3,
		CollectionID: 8,
		Key:          []byte("doc"),
		Value:        value,
		Datatype:     uint8(memd.DatatypeFlagXattrs),
	})

	event := obs.events[docKey{collectionID: 8, id: "doc"}]
	if event.deleted {
		t.Fatal("a mutation was recorded as a deletion")
	}
	if !event.xattrsKnown {
		t.Fatal("the xattrs of a mutation must be known")
	}
	assertNames(t, event.xattrs, []string{"_sync", "user"})
}

func TestObserverRecordsTombstoneXattrs(t *testing.T) {
	obs := newObserver()
	obs.Deletion(gocbcore.DcpDeletion{
		SeqNo:        4,
		CollectionID: 8,
		Key:          []byte("doc"),
		Value:        encodeXattrs([][2]string{{"_sync", "{}"}}, ""),
		Datatype:     uint8(memd.DatatypeFlagXattrs),
	})

	event := obs.events[docKey{collectionID: 8, id: "doc"}]
	if !event.deleted || !event.xattrsKnown {
		t.Fatalf("got %+v, want a deletion with known xattrs", event)
	}
	assertNames(t, event.xattrs, []string{"_sync"})
}

// An expiration carries no value, so the purge must read the xattr names back rather than
// assume the document has none.
func TestObserverMarksExpirationXattrsUnknown(t *testing.T) {
	obs := newObserver()
	key := docKey{collectionID: 8, id: "doc"}
	obs.Mutation(gocbcore.DcpMutation{
		SeqNo:        1,
		CollectionID: 8,
		Key:          []byte("doc"),
		Value:        encodeXattrs([][2]string{{"_sync", "{}"}}, `{"a":1}`),
		Datatype:     uint8(memd.DatatypeFlagXattrs),
	})
	obs.Expiration(gocbcore.DcpExpiration{SeqNo: 2, CollectionID: 8, Key: []byte("doc")})

	event := obs.events[key]
	if !event.deleted {
		t.Fatal("an expiration must be recorded as a deletion")
	}
	if event.xattrsKnown {
		t.Fatal("an expiration reports no value, so its xattrs cannot be known")
	}
}

func TestObserverKeepsTheNewestEvent(t *testing.T) {
	obs := newObserver()
	key := docKey{collectionID: 8, id: "doc"}
	obs.Deletion(gocbcore.DcpDeletion{SeqNo: 9, CollectionID: 8, Key: []byte("doc")})
	obs.Mutation(gocbcore.DcpMutation{SeqNo: 2, CollectionID: 8, Key: []byte("doc")})

	if event := obs.events[key]; !event.deleted || event.seqNo != 9 {
		t.Fatalf("got %+v, want the deletion at sequence 9", event)
	}
}

func TestObserverReportsABadXattrSection(t *testing.T) {
	obs := newObserver()
	obs.Mutation(gocbcore.DcpMutation{
		SeqNo:        1,
		CollectionID: 8,
		Key:          []byte("doc"),
		Value:        []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00},
		Datatype:     uint8(memd.DatatypeFlagXattrs),
	})

	if err := obs.err(); err == nil {
		t.Fatal("expected an error for an undecodable xattr section")
	}
	if len(obs.events) != 0 {
		t.Fatalf("got %d events, want none recorded for an undecodable value", len(obs.events))
	}
}

func assertNames(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
