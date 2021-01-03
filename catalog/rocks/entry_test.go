package rocks

import (
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEntryToValueAndBack(t *testing.T) {
	// convert entry to value and back
	now := time.Now()
	entry := &Entry{
		Address:      "entry1",
		LastModified: timestamppb.New(now),
		Size:         99,
		ETag:         "123456789",
		Metadata:     map[string]string{"key9": "value9", "key1": "value1"},
	}
	val, err := EntryToValue(entry)
	testutil.MustDo(t, "convert entry value", err)

	if len(val.Identity) == 0 {
		t.Error("EntryToValue() missing identify")
	}

	ent, err := ValueToEntry(val)
	testutil.MustDo(t, "convert value to entry", err)

	if diff := deep.Equal(entry, ent); diff != nil {
		t.Fatal("Entry convert to value and back failed:", diff)
	}
}
