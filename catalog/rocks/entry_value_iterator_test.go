package rocks_test

import (
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/catalog/rocks"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/catalog/rocks/testutils"
	"github.com/treeverse/lakefs/graveler"
)

func TestNewEntryToValueIterator(t *testing.T) {
	var expectedRecords []*graveler.ValueRecord
	var entryRecords []*rocks.EntryRecord

	// generate data
	for i := 0; i < 3; i++ {
		record := &rocks.EntryRecord{
			Path:  rocks.Path(fmt.Sprintf("path%d", i)),
			Entry: &rocks.Entry{Address: fmt.Sprintf("addr%d", i)},
		}
		entryRecords = append(entryRecords, record)
		expectedRecords = append(expectedRecords, &graveler.ValueRecord{
			Key:   []byte(record.Path.String()),
			Value: rocks.MustEntryToValue(record.Entry),
		})
	}

	// collect the above using entry to value iterator
	entryIterator := testutils.NewFakeEntryIterator(entryRecords)
	it := rocks.NewEntryToValueIterator(entryIterator)
	defer it.Close()

	var values []*graveler.ValueRecord
	for it.Next() {
		values = append(values, it.Value())
	}
	if err := it.Err(); err != nil {
		t.Fatal("Iterator ended with an error", err)
	}

	// verify that what we produced is what we got from the iterator
	if diff := deep.Equal(values, expectedRecords); diff != nil {
		t.Fatal("EntryToValue iterator found diff:", diff)
	}
}
