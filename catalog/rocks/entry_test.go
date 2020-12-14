package rocks

import (
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValueToEntry_Tombstone(t *testing.T) {
	ent, err := ValueToEntry(TombstoneValue)
	testutil.MustDo(t, "convert empty value to entry", err)
	if ent != nil {
		t.Fatalf("ValueToEntry() with tombstone should return nil, got %v", ent)
	}
}

func TestEntryToValue_Tombstone(t *testing.T) {
	val, err := EntryToValue(nil)
	testutil.MustDo(t, "convert tombstone entry to value", err)
	if !IsTombstoneValue(val) {
		t.Fatalf("EntryToValue() with nil entry should return tombstone value: %v", val)
	}
}

func TestEntryToValueAndBack(t *testing.T) {
	// convert entry to value and back
	now := time.Now()
	entry := &Entry{
		Address:      "entry1",
		LastModified: timestamppb.New(now),
		Size:         99,
		ETag:         []byte("\xba\xdc\x0f\xfe"),
		Metadata:     nil,
	}
	val, err := EntryToValue(entry)
	testutil.MustDo(t, "convert entry value", err)

	if IsTombstoneValue(val) {
		t.Fatal("EntryToValue with entry should not return tombstone")
	}

	ent, err := ValueToEntry(val)
	testutil.MustDo(t, "convert value to entry", err)

	if diff := deep.Equal(entry, ent); diff != nil {
		t.Fatal("Entry convert to value and back failed:", diff)
	}
}
