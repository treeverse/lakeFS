package catalog

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// sortedEntryDiffIterator is an in-memory EntryDiffIterator over sorted paths.
type sortedEntryDiffIterator struct {
	paths []string
	pos   int // index of the next path to return
	value *EntryDiff
}

func newSortedEntryDiffIterator(paths []string) *sortedEntryDiffIterator {
	sorted := append([]string(nil), paths...)
	sort.Strings(sorted)
	return &sortedEntryDiffIterator{paths: sorted}
}

func (it *sortedEntryDiffIterator) Next() bool {
	if it.pos >= len(it.paths) {
		it.value = nil
		return false
	}
	it.value = &EntryDiff{
		Type:  graveler.DiffTypeAdded,
		Path:  Path(it.paths[it.pos]),
		Entry: &Entry{Address: "addr"},
	}
	it.pos++
	return true
}

func (it *sortedEntryDiffIterator) SeekGE(id Path) {
	it.pos = sort.SearchStrings(it.paths, id.String())
	it.value = nil
}

func (it *sortedEntryDiffIterator) Value() *EntryDiff { return it.value }
func (it *sortedEntryDiffIterator) Err() error        { return nil }
func (it *sortedEntryDiffIterator) Close()            {}

// listAllDiffPages walks all pages like a client does: the last path of a page is "after" of the next one.
func listAllDiffPages(t *testing.T, paths []string, prefix, delimiter string, limit int) []string {
	t.Helper()
	var got []string
	after := ""
	for range len(paths) + 2 {
		diffs, hasMore, err := listDiffHelper(newSortedEntryDiffIterator(paths), prefix, delimiter, limit, after)
		require.NoError(t, err)
		for _, d := range diffs {
			got = append(got, d.Path)
		}
		if !hasMore {
			return got
		}
		after = diffs[len(diffs)-1].Path
	}
	t.Fatal("paging did not finish")
	return nil
}

func TestListDiffHelper_PagingWithDelimiter(t *testing.T) {
	t.Parallel()

	// t/part-0 .. t/part-109: a page can end on t/part-9, and t/part-90..99 start with it
	var manyParts []string
	for i := range 110 {
		manyParts = append(manyParts, fmt.Sprintf("t/part-%d", i))
	}

	cases := []struct {
		name      string
		paths     []string
		prefix    string
		delimiter string
		limit     int
		want      []string
	}{
		{
			name:      "objects_with_longer_siblings",
			paths:     manyParts,
			prefix:    "t/",
			delimiter: "/",
			limit:     10,
			want: func() []string {
				all := append([]string(nil), manyParts...)
				sort.Strings(all)
				return all
			}(),
		},
		{
			name:      "common_prefixes_still_skipped_as_a_whole",
			paths:     append([]string{"u/x", "v/y"}, manyParts...),
			prefix:    "",
			delimiter: "/",
			limit:     1,
			want:      []string{"t/", "u/", "v/"},
		},
		{
			name:      "object_and_common_prefix_with_same_name",
			paths:     []string{"d/f", "d/f/a", "d/f/b", "d/g"},
			prefix:    "d/",
			delimiter: "/",
			limit:     1,
			want:      []string{"d/f", "d/f/", "d/g"},
		},
		{
			name:      "no_delimiter",
			paths:     manyParts,
			prefix:    "t/",
			delimiter: "",
			limit:     10,
			want: func() []string {
				all := append([]string(nil), manyParts...)
				sort.Strings(all)
				return all
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := listAllDiffPages(t, tc.paths, tc.prefix, tc.delimiter, tc.limit)
			require.Equal(t, tc.want, got)
		})
	}
}
