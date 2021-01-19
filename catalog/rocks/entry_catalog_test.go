package rocks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEntryCatalog_GetEntry_NotFound(t *testing.T) {
	gravelerMock := &FakeGraveler{Err: graveler.ErrNotFound}
	cat := EntryCatalog{Store: gravelerMock}
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
	cat := EntryCatalog{Store: gravelerMock}
	ctx := context.Background()
	got, err := cat.GetEntry(ctx, "repo1", "master", "file1")
	testutil.MustDo(t, "get entry", err)
	if diff := deep.Equal(entry, got); diff != nil {
		t.Fatal("GetEntry() got entry with diff", diff)
	}
}

func TestEntryCatalog_SetEntry(t *testing.T) {
	gravelerMock := &FakeGraveler{KeyValue: make(map[string]*graveler.Value)}
	cat := EntryCatalog{Store: gravelerMock}
	ctx := context.Background()
	var (
		addr           = "addr1"
		now            = time.Now()
		size     int64 = 1234
		tag            = "quick brown fox"
		metadata       = map[string]string{"one": "1", "two": "2"}
	)
	entry := &Entry{
		Address:      addr,
		LastModified: timestamppb.New(now),
		Size:         size,
		ETag:         tag,
		Metadata:     metadata,
	}
	err := cat.SetEntry(ctx, "repo", "ref", "path1", entry)
	testutil.MustDo(t, "set entry", err)
	// verify that mock got the right entry
	got, err := cat.GetEntry(ctx, "repo", "ref", "path1")
	testutil.MustDo(t, "get entry we just set", err)
	if diff := deep.Equal(entry, got); diff != nil {
		t.Fatal("GetEntry() got entry with after set diff", diff)
	}
}

func TestEntryCatalog_ListEntries_NoDelimiter(t *testing.T) {
	entriesData := []*Entry{{Address: "addr1", Size: 1}, nil, nil}
	listingData := []*graveler.ValueRecord{
		{Key: graveler.Key("file1"), Value: MustEntryToValue(entriesData[0])},
		{Key: graveler.Key("file2"), Value: MustEntryToValue(entriesData[1])},
		{Key: graveler.Key("file3"), Value: MustEntryToValue(entriesData[2])},
	}
	gravelerMock := &FakeGraveler{
		ListIteratorFactory: NewFakeValueIteratorFactory(listingData),
	}
	cat := EntryCatalog{Store: gravelerMock}
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
}

func TestEntryCatalog_ListEntries_WithDelimiter(t *testing.T) {
	// prepare data
	var gravelerData []*graveler.ValueRecord
	for _, name := range []string{"file1", "folder/file1", "folder/file2", "zzz"} {
		entry := &Entry{Address: name}
		record := &graveler.ValueRecord{Value: MustEntryToValue(entry), Key: graveler.Key(name)}
		gravelerData = append(gravelerData, record)
	}
	gravelerMock := &FakeGraveler{
		ListIteratorFactory: NewFakeValueIteratorFactory(gravelerData),
	}
	cat := EntryCatalog{Store: gravelerMock}
	ctx := context.Background()

	t.Run("root", func(t *testing.T) {
		entries, err := cat.ListEntries(ctx, "repo", "ref", "", "/")
		testutil.MustDo(t, "list entries", err)
		defer entries.Close()
		// listing entries
		expected := []*EntryListing{
			{Path: "file1", Entry: &Entry{Address: "file1"}},
			{CommonPrefix: true, Path: "folder/"},
			{CommonPrefix: false, Path: "zzz", Entry: &Entry{Address: "zzz"}},
		}

		// collect and compare
		var listing []*EntryListing
		for entries.Next() {
			listing = append(listing, entries.Value())
		}
		if diff := deep.Equal(listing, expected); diff != nil {
			t.Fatal("List entries diff found:", diff)
		}
	})
	t.Run("folder", func(t *testing.T) {
		entries, err := cat.ListEntries(ctx, "repo", "ref", "folder/", "/")
		testutil.MustDo(t, "list entries", err)
		defer entries.Close()
		// listing entries
		expected := []*EntryListing{
			{Path: "folder/file1", Entry: &Entry{Address: "folder/file1"}},
			{Path: "folder/file2", Entry: &Entry{Address: "folder/file2"}},
		}

		// collect and compare
		var listing []*EntryListing
		for entries.Next() {
			listing = append(listing, entries.Value())
		}
		if diff := deep.Equal(listing, expected); diff != nil {
			t.Fatal("List entries diff found:", diff)
		}
	})
}

func TestEntryCatalog_Diff(t *testing.T) {
	entriesData := []*Entry{{Address: "addr2", Size: 2}, nil, nil}
	diffData := []*graveler.Diff{
		{Type: graveler.DiffTypeAdded, Key: graveler.Key("file1"), Value: MustEntryToValue(entriesData[0])},
		{Type: graveler.DiffTypeRemoved, Key: graveler.Key("file2"), Value: MustEntryToValue(entriesData[1])},
		{Type: graveler.DiffTypeChanged, Key: graveler.Key("file3"), Value: MustEntryToValue(entriesData[2])},
	}
	gravelerMock := &FakeGraveler{
		DiffIteratorFactory: NewFakeDiffIteratorFactory(diffData),
	}
	cat := EntryCatalog{Store: gravelerMock}
	ctx := context.Background()
	diffs, err := cat.Diff(ctx, "repo", "left", "right")
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
