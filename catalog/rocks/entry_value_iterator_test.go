package rocks

import (
	"fmt"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
)

func TestNewEntryToValueIterator(t *testing.T) {
	var expectedRecords []*graveler.ValueRecord
	var entryRecords []*EntryRecord

	// generate data
	for i := 0; i < 3; i++ {
		record := &EntryRecord{
			Path:  Path(fmt.Sprintf("path%d", i)),
			Entry: &Entry{Address: fmt.Sprintf("addr%d", i)},
		}
		entryRecords = append(entryRecords, record)
		expectedRecords = append(expectedRecords, &graveler.ValueRecord{
			Key:   []byte(record.Path.String()),
			Value: MustEntryToValue(record.Entry),
		})
	}

	// collect the above using entry to value iterator
	entryIterator := NewFakeEntryIterator(entryRecords)
	it := NewEntryToValueIterator(entryIterator)
	defer it.Close()

	var values []*graveler.ValueRecord
	for it.Next() {
		values = append(values, it.Value())
	}
	// verify that what we produced is what we got from the iterator
	if diff := deep.Equal(values, expectedRecords); diff != nil {
		t.Fatal("ValueToEntry iterator found diff:", diff)
	}
}
