package onboard_test

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/onboard"
)

type mockCataloger struct {
	catalog.Cataloger
}

var catalogCallData = struct {
	addedEntries   []catalog.Entry
	deletedEntries []string
	callLog        map[string]int
}{}

func (m mockCataloger) CreateEntries(_ context.Context, _, _ string, entries []catalog.Entry) error {
	catalogCallData.addedEntries = append(catalogCallData.addedEntries, entries...)
	catalogCallData.callLog["CreateEntries"]++
	return nil
}
func (m mockCataloger) DeleteEntry(_ context.Context, _, _ string, path string) error {
	catalogCallData.deletedEntries = append(catalogCallData.deletedEntries, path)
	catalogCallData.callLog["DeleteEntry"]++
	return nil
}

func TestCreateAndDeleteRows(t *testing.T) {
	c := onboard.NewCatalogActions(mockCataloger{}, "example-repo", "committer", 5)
	catalogActions, ok := c.(*onboard.CatalogRepoActions)
	if !ok {
		t.Fatal("NewCatalogActions return value implement doesn't match")
	}
	testdata := []struct {
		AddedRows           []string
		DeletedRows         []string
		ExpectedAddCalls    int
		ExpectedDeleteCalls int
	}{
		{
			AddedRows:           []string{"a1", "b2", "c3"},
			DeletedRows:         []string{},
			ExpectedAddCalls:    1,
			ExpectedDeleteCalls: 0,
		},
		{
			AddedRows:           []string{"a1", "b2", "c3"},
			DeletedRows:         []string{"b1", "b2"},
			ExpectedAddCalls:    1,
			ExpectedDeleteCalls: 2,
		},
		{
			AddedRows:           []string{"a1", "b2", "c3", "d4", "e5"},
			DeletedRows:         []string{"b1", "b2"},
			ExpectedAddCalls:    1,
			ExpectedDeleteCalls: 2,
		},
		{
			AddedRows:           []string{"a1", "b2", "c3", "d4", "e5", "f6"},
			DeletedRows:         []string{"b1", "b2"},
			ExpectedAddCalls:    2,
			ExpectedDeleteCalls: 2,
		},
		{
			AddedRows:           []string{"a1", "b2", "c3", "d4", "e5", "f6"},
			DeletedRows:         []string{},
			ExpectedAddCalls:    2,
			ExpectedDeleteCalls: 0,
		},
		{
			AddedRows:           []string{"a1", "b2", "c3", "d4", "e5", "f6", "g7", "h8", "i9", "j0"},
			DeletedRows:         []string{},
			ExpectedAddCalls:    2,
			ExpectedDeleteCalls: 0,
		},
		{
			AddedRows:           []string{},
			DeletedRows:         []string{"a1", "b2"},
			ExpectedAddCalls:    0,
			ExpectedDeleteCalls: 2,
		},
	}
	for _, test := range testdata {
		catalogCallData.addedEntries = []catalog.Entry{}
		catalogCallData.deletedEntries = []string{}
		catalogCallData.callLog = make(map[string]int)
		err := catalogActions.CreateAndDeleteObjects(context.Background(), rows(test.AddedRows...), rows(test.DeletedRows...))
		if err != nil {
			t.Fatalf("failed to create/delete objects: %v", err)
		}
		if catalogCallData.callLog["CreateEntries"] != test.ExpectedAddCalls {
			t.Fatalf("unexpected number of CreateEntries calls. expected=%d, got=%d", test.ExpectedAddCalls, catalogCallData.callLog["CreateEntries"])
		}
		if catalogCallData.callLog["DeleteEntry"] != test.ExpectedDeleteCalls {
			t.Fatalf("unexpected number of DeleteEntries calls. expected=%d, got=%d", test.ExpectedDeleteCalls, catalogCallData.callLog["DeleteEntry"])
		}
		if len(catalogCallData.addedEntries) != len(test.AddedRows) {
			t.Fatalf("unexpected number of added entries. expected=%d, got=%d", len(test.AddedRows), len(catalogCallData.addedEntries))
		}
		for i, entry := range catalogCallData.addedEntries {
			if entry.Path != test.AddedRows[i] {
				t.Fatalf("unexpected added entry at index %d: expected=%s, got=%s", i, test.AddedRows[i], entry.Path)
			}
		}
		if len(catalogCallData.deletedEntries) != len(test.DeletedRows) {
			t.Fatalf("unexpected number of deleted entries. expected=%d, got=%d", len(test.DeletedRows), len(catalogCallData.deletedEntries))
		}
		for i, path := range catalogCallData.deletedEntries {
			if path != test.DeletedRows[i] {
				t.Fatalf("unexpected deleted entry at index %d: expected=%s, got=%s", i, test.AddedRows[i], path)
			}
		}
	}
}
