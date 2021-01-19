package rocks_test

import (
	"testing"

	"github.com/treeverse/lakefs/catalog/rocks"

	"github.com/treeverse/lakefs/catalog/rocks/testutils"

	"github.com/go-test/deep"
)

func TestEntryPrefixIterator(t *testing.T) {
	tests := []struct {
		name            string
		prefix          rocks.Path
		from            rocks.Path
		entriesIterator rocks.EntryIterator
		expected        []*rocks.EntryRecord
	}{
		{
			name: "no prefix",
			entriesIterator: testutils.NewFakeEntryIterator([]*rocks.EntryRecord{
				{Path: "path1", Entry: &rocks.Entry{Address: "address1"}},
			}),
			expected: []*rocks.EntryRecord{
				{Path: "path1", Entry: &rocks.Entry{Address: "address1"}},
			},
		},
		{
			name:   "no files",
			prefix: "path/",
			entriesIterator: testutils.NewFakeEntryIterator([]*rocks.EntryRecord{
				{Path: "other/path/foo", Entry: &rocks.Entry{Address: "other/path/foo"}},
			}),
		},
		{
			name:   "one file",
			prefix: "path/",
			entriesIterator: testutils.NewFakeEntryIterator([]*rocks.EntryRecord{
				{Path: "path/foo", Entry: &rocks.Entry{Address: "path/foo"}},
			}),
			expected: []*rocks.EntryRecord{
				{Path: "path/foo", Entry: &rocks.Entry{Address: "path/foo"}},
			},
		},
		{
			name:   "one file in prefix",
			prefix: "path/",
			entriesIterator: testutils.NewFakeEntryIterator([]*rocks.EntryRecord{
				{Path: "before/foo", Entry: &rocks.Entry{Address: "before/foo"}},
				{Path: "path/foo", Entry: &rocks.Entry{Address: "path/foo"}},
				{Path: "last/foo", Entry: &rocks.Entry{Address: "last/foo"}},
			}),
			expected: []*rocks.EntryRecord{
				{Path: "path/foo", Entry: &rocks.Entry{Address: "path/foo"}},
			},
		},
		{
			name:   "seek before",
			prefix: "path/",
			from:   "before/",
			entriesIterator: testutils.NewFakeEntryIterator([]*rocks.EntryRecord{
				{Path: "before/foo", Entry: &rocks.Entry{Address: "before/foo"}},
				{Path: "path/foo", Entry: &rocks.Entry{Address: "path/foo"}},
				{Path: "last/foo", Entry: &rocks.Entry{Address: "last/foo"}},
			}),
			expected: []*rocks.EntryRecord{
				{Path: "path/foo", Entry: &rocks.Entry{Address: "path/foo"}},
			},
		},
		{
			name:   "seek after",
			prefix: "path/",
			from:   "z_after/",
			entriesIterator: testutils.NewFakeEntryIterator([]*rocks.EntryRecord{
				{Path: "before/foo", Entry: &rocks.Entry{Address: "before/foo"}},
				{Path: "path/foo", Entry: &rocks.Entry{Address: "path/foo"}},
				{Path: "z_after/foo", Entry: &rocks.Entry{Address: "z_after/foo"}},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefixIter := rocks.NewPrefixIterator(tt.entriesIterator, tt.prefix)
			defer prefixIter.Close()
			prefixIter.SeekGE(tt.from)

			// collect entries from iterator
			var entries []*rocks.EntryRecord
			for prefixIter.Next() {
				entries = append(entries, prefixIter.Value())
			}

			// compare with expected
			if diff := deep.Equal(entries, tt.expected); diff != nil {
				t.Fatal("Prefix iterator found diff in result:", diff)
			}
		})
	}
}
