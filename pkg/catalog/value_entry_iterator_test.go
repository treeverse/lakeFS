package catalog

import (
	"fmt"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func TestNewValueToEntryIterator(t *testing.T) {
	var expectedRecords []*EntryRecord
	var valueRecords []*graveler.ValueRecord

	// generate data
	for i := 0; i < 3; i++ {
		record := &EntryRecord{
			Path:  Path(fmt.Sprintf("path%d", i)),
			Entry: &Entry{Address: fmt.Sprintf("addr%d", i)},
		}
		expectedRecords = append(expectedRecords, record)
		valueRecords = append(valueRecords, &graveler.ValueRecord{
			Key:   []byte(record.Path.String()),
			Value: MustEntryToValue(record.Entry),
		})
	}

	// collect the above using value to entry iterator
	valueIter := NewFakeValueIterator(valueRecords)
	it := NewValueToEntryIterator(valueIter)
	defer it.Close()

	var entries []*EntryRecord
	for it.Next() {
		entries = append(entries, it.Value())
	}
	// verify that what we produced is what we got from the iterator
	if diff := deep.Equal(entries, expectedRecords); diff != nil {
		t.Fatal("ValueToEntry iterator found diff:", diff)
	}
}
