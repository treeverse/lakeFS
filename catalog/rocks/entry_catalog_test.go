package rocks

import (
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/testutil"
)

func TestEntryCatalog_GetEntry_NotFound(t *testing.T) {
	gravelerMock := &FakeGraveler{Err: graveler.ErrNotFound}
	cat := entryCatalog{store: gravelerMock}
	ctx := context.Background()
	got, err := cat.GetEntry(ctx, "repo", "ref", "path1")
	if !errors.Is(err, graveler.ErrNotFound) {
		t.Fatalf("GetEntry() got err = %v, expected error not found", err)
	}
	if got != nil {
		t.Fatalf("GetEntry() got entry = %v, expected nil", got)
	}
}

func TestEntryCatalog_GetEntry_Found(t *testing.T) {
	entry := &Entry{Address: "addr1"}
	gravelerMock := &FakeGraveler{KeyValue: map[string]*graveler.Value{"repo1/master/file1": MustEntryToValue(entry)}}
	cat := entryCatalog{store: gravelerMock}
	ctx := context.Background()
	got, err := cat.GetEntry(ctx, "repo1", "master", "file1")
	testutil.MustDo(t, "get entry", err)
	if diff := deep.Equal(entry, got); diff != nil {
		t.Fatal("GetEntry() got entry with diff", diff)
	}
}

func TestEntryCatalog_SetEntry(t *testing.T) {
	gravelerMock := &FakeGraveler{KeyValue: make(map[string]*graveler.Value)}
	cat := entryCatalog{store: gravelerMock}
	ctx := context.Background()
	entry := &Entry{Address: "addr1"}
	err := cat.SetEntry(ctx, "repo", "ref", "path1", entry)
	testutil.MustDo(t, "set entry", err)
	// verify that mock got the right entry
	got, err := cat.GetEntry(ctx, "repo", "ref", "path1")
	testutil.MustDo(t, "get entry we just set", err)
	if diff := deep.Equal(entry, got); diff != nil {
		t.Fatal("GetEntry() got entry with after set diff", diff)
	}
}

func TestEntryCatalog_ListEntries(t *testing.T) {
	entriesData := []*Entry{{Address: "addr1", Size: 1}, nil, nil}
	listingData := []*graveler.ValueRecord{
		{Key: graveler.Key("file1"), Value: MustEntryToValue(entriesData[0])},
		{Key: graveler.Key("file2"), Value: MustEntryToValue(entriesData[1])},
		{Key: graveler.Key("file3"), Value: MustEntryToValue(entriesData[2])},
	}
	gravelerMock := &FakeGraveler{
		ListIterator: NewFakeValueIterator(listingData),
	}
	cat := entryCatalog{store: gravelerMock}
	ctx := context.Background()
	entries, err := cat.ListEntries(ctx, "repo", "ref", "", "")
	testutil.MustDo(t, "list entries", err)
	defer entries.Close()

	var i int
	for i = 0; entries.Next(); i++ {
		v := entries.Value()
		key := listingData[i].Key
		if v.Path.String() != key.String() {
			t.Errorf("ListEntries() at %d path %s, expected %s", i, v.Path, key)
		}
		if diff := deep.Equal(entriesData[i], v.Entry); diff != nil {
			t.Errorf("ListEntries() at %d found diff %s", i, diff)
		}
	}
	if i != len(listingData) {
		t.Fatalf("ListEntries() got %d entries, expected %d", i, len(listingData))
	}

	// TODO(barak): test listing with prefix and testing with '/' delimiter
}

func TestEntryCatalog_Diff(t *testing.T) {
	entriesData := []*Entry{{Address: "addr2", Size: 2}, nil, nil}
	diffData := []*graveler.Diff{
		{Type: graveler.DiffTypeAdded, Key: graveler.Key("file1"), Value: MustEntryToValue(entriesData[0])},
		{Type: graveler.DiffTypeRemoved, Key: graveler.Key("file2"), Value: MustEntryToValue(entriesData[1])},
		{Type: graveler.DiffTypeChanged, Key: graveler.Key("file3"), Value: MustEntryToValue(entriesData[2])},
	}
	gravelerMock := &FakeGraveler{
		DiffIterator: NewFakeDiffIterator(diffData),
	}
	cat := entryCatalog{store: gravelerMock}
	ctx := context.Background()
	diffs, err := cat.Diff(ctx, "repo", "left", "right", "")
	testutil.MustDo(t, "diff", err)
	defer diffs.Close()

	var i int
	for i = 0; diffs.Next(); i++ {
		v := diffs.Value()
		data := diffData[i]
		if v.Path.String() != data.Key.String() {
			t.Errorf("Diff() at %d path %s, expected %s", i, v.Path, data.Key)
		}
		if v.Type != data.Type {
			t.Errorf("Diff() at %d type %v, expected %v", i, v.Type, data.Type)
		}
		if diff := deep.Equal(entriesData[i], v.Entry); diff != nil {
			t.Errorf("Diff() at %d found diff %s", i, diff)
		}
	}
	if i != len(diffData) {
		t.Fatalf("Diff() got %d diffs, expected %d", i, len(diffData))
	}
}
