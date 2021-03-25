package catalog_test

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
)

func TestEntryPrefixIterator(t *testing.T) {
	tests := []struct {
		name            string
		prefix          catalog.Path
		from            catalog.Path
		entriesIterator catalog.EntryIterator
		expected        []*catalog.EntryRecord
	}{
		{
			name: "no prefix",
			entriesIterator: testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
				{Path: "path1", Entry: &catalog.Entry{Address: "address1"}},
			}),
			expected: []*catalog.EntryRecord{
				{Path: "path1", Entry: &catalog.Entry{Address: "address1"}},
			},
		},
		{
			name:   "no files",
			prefix: "path/",
			entriesIterator: testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
				{Path: "other/path/foo", Entry: &catalog.Entry{Address: "other/path/foo"}},
			}),
		},
		{
			name:   "one file",
			prefix: "path/",
			entriesIterator: testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
				{Path: "path/foo", Entry: &catalog.Entry{Address: "path/foo"}},
			}),
			expected: []*catalog.EntryRecord{
				{Path: "path/foo", Entry: &catalog.Entry{Address: "path/foo"}},
			},
		},
		{
			name:   "one file in prefix",
			prefix: "path/",
			entriesIterator: testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
				{Path: "before/foo", Entry: &catalog.Entry{Address: "before/foo"}},
				{Path: "path/foo", Entry: &catalog.Entry{Address: "path/foo"}},
				{Path: "last/foo", Entry: &catalog.Entry{Address: "last/foo"}},
			}),
			expected: []*catalog.EntryRecord{
				{Path: "path/foo", Entry: &catalog.Entry{Address: "path/foo"}},
			},
		},
		{
			name:   "seek before",
			prefix: "path/",
			from:   "before/",
			entriesIterator: testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
				{Path: "before/foo", Entry: &catalog.Entry{Address: "before/foo"}},
				{Path: "path/foo", Entry: &catalog.Entry{Address: "path/foo"}},
				{Path: "last/foo", Entry: &catalog.Entry{Address: "last/foo"}},
			}),
			expected: []*catalog.EntryRecord{
				{Path: "path/foo", Entry: &catalog.Entry{Address: "path/foo"}},
			},
		},
		{
			name:   "seek after",
			prefix: "path/",
			from:   "z_after/",
			entriesIterator: testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
				{Path: "before/foo", Entry: &catalog.Entry{Address: "before/foo"}},
				{Path: "path/foo", Entry: &catalog.Entry{Address: "path/foo"}},
				{Path: "z_after/foo", Entry: &catalog.Entry{Address: "z_after/foo"}},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefixIter := catalog.NewPrefixIterator(tt.entriesIterator, tt.prefix)
			defer prefixIter.Close()
			prefixIter.SeekGE(tt.from)

			// collect entries from iterator
			var entries []*catalog.EntryRecord
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
