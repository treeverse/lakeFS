package onboard

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/treeverse/lakefs/catalog/rocks/testutils"

	"github.com/treeverse/lakefs/catalog/rocks"
)

func TestMergeIterator(t *testing.T) {
	cases := []struct {
		Name        string
		InvIt       rocks.EntryIterator
		CommittedIt rocks.EntryListingIterator
		Prefixes    []string
		Results     []string
	}{
		{
			Name: "Simple prefix",
			InvIt: testutils.NewFakeEntryIterator([]*rocks.EntryRecord{
				{Path: "a/b/c"},
				{Path: "a/c/d"},
				{Path: "b/c/d"},
				{Path: "d/c"},
			}),
			CommittedIt: rocks.NewEntryListingIterator(testutils.NewFakeEntryIterator(
				[]*rocks.EntryRecord{
					{Path: "a/b/d"},
					{Path: "a/b/c/d"},
					{Path: "a/c/d"},
					{Path: "b/c/d"},
				}), "", ""),
			Prefixes: []string{"a/"},
			Results:  []string{"a/b/c", "a/c/d", "b/c/d"},
		},
		{
			Name: "No prefixes - All inventory",
			InvIt: testutils.NewFakeEntryIterator([]*rocks.EntryRecord{
				{Path: "a/b/c"},
				{Path: "e/f/s/a"},
			}),
			CommittedIt: rocks.NewEntryListingIterator(testutils.NewFakeEntryIterator(
				[]*rocks.EntryRecord{
					{Path: "some/other/prefix"},
					{Path: "some/other/prefix/c"},
					{Path: "some/other/prefix/e"},
				}), "", ""),
			Prefixes: []string{},
			Results:  []string{"a/b/c", "e/f/s/a"},
		},
		{
			Name: "Multiple prefixes",
			InvIt: testutils.NewFakeEntryIterator([]*rocks.EntryRecord{
				{Path: "a/b/c"},
				{Path: "a/d/c"},
				{Path: "f/t/y"},
			}),
			CommittedIt: rocks.NewEntryListingIterator(testutils.NewFakeEntryIterator(
				[]*rocks.EntryRecord{
					{Path: "a/b/c"},
					{Path: "a/b/c/d"},
					{Path: "a/c/d"},
					{Path: "b/c/d"},
					{Path: "e/a/b"},
					{Path: "f/c/d"},
					{Path: "g/x/y"},
				}), "", ""),
			Prefixes: []string{"a/", "e/", "f/"},
			Results:  []string{"a/b/c", "a/d/c", "b/c/d", "f/t/y", "g/x/y"},
		},
	}

	for _, c := range cases {
		sut := newPrefixMergeIterator(c.InvIt, c.CommittedIt, c.Prefixes)

		count := 0
		for ; sut.Next(); count++ {
			require.Equal(t, c.Results[count], string(sut.Value().Path))
		}

		require.NoError(t, sut.Err())
		require.Equal(t, len(c.Results), count)

		// no more values after next return false
		require.Nil(t, sut.Value())

		sut.Close()
		require.NoError(t, sut.Err())
	}
}
