package rocks

import (
	"testing"

	"github.com/go-test/deep"
)

func TestEntryPrefixIterator(t *testing.T) {
	tests := []struct {
		name            string
		prefix          Path
		from            Path
		entriesIterator EntryIterator
		expected        []*EntryRecord
	}{
		{
			name: "no prefix",
			entriesIterator: NewFakeEntryIterator([]*EntryRecord{
				{Path: "path1", Entry: &Entry{Address: "address1"}},
			}),
			expected: []*EntryRecord{
				{Path: "path1", Entry: &Entry{Address: "address1"}},
			},
		},
		{
			name:   "no files",
			prefix: "path/",
			entriesIterator: NewFakeEntryIterator([]*EntryRecord{
				{Path: "other/path/foo", Entry: &Entry{Address: "other/path/foo"}},
			}),
		},
		{
			name:   "one file",
			prefix: "path/",
			entriesIterator: NewFakeEntryIterator([]*EntryRecord{
				{Path: "path/foo", Entry: &Entry{Address: "path/foo"}},
			}),
			expected: []*EntryRecord{
				{Path: "path/foo", Entry: &Entry{Address: "path/foo"}},
			},
		},
		{
			name:   "one file in prefix",
			prefix: "path/",
			entriesIterator: NewFakeEntryIterator([]*EntryRecord{
				{Path: "before/foo", Entry: &Entry{Address: "before/foo"}},
				{Path: "path/foo", Entry: &Entry{Address: "path/foo"}},
				{Path: "last/foo", Entry: &Entry{Address: "last/foo"}},
			}),
			expected: []*EntryRecord{
				{Path: "path/foo", Entry: &Entry{Address: "path/foo"}},
			},
		},
		{
			name:   "seek before",
			prefix: "path/",
			from:   "before/",
			entriesIterator: NewFakeEntryIterator([]*EntryRecord{
				{Path: "before/foo", Entry: &Entry{Address: "before/foo"}},
				{Path: "path/foo", Entry: &Entry{Address: "path/foo"}},
				{Path: "last/foo", Entry: &Entry{Address: "last/foo"}},
			}),
			expected: []*EntryRecord{
				{Path: "path/foo", Entry: &Entry{Address: "path/foo"}},
			},
		},
		{
			name:   "seek after",
			prefix: "path/",
			from:   "z_after/",
			entriesIterator: NewFakeEntryIterator([]*EntryRecord{
				{Path: "before/foo", Entry: &Entry{Address: "before/foo"}},
				{Path: "path/foo", Entry: &Entry{Address: "path/foo"}},
				{Path: "z_after/foo", Entry: &Entry{Address: "z_after/foo"}},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefixIter := NewPrefixIterator(tt.entriesIterator, tt.prefix)
			defer prefixIter.Close()
			prefixIter.SeekGE(tt.from)

			// collect entries from iterator
			var entries []*EntryRecord
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
