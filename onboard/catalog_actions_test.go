package onboard_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
)

type mockCataloger struct {
	catalog.Cataloger
}

var catalogCallData = struct {
	addedEntries   map[string]bool
	deletedEntries map[string]bool
	callLog        map[string]*int32
	mux            sync.Mutex
}{}

func (m mockCataloger) CreateEntries(_ context.Context, _, _ string, entries []catalog.Entry) error {
	catalogCallData.mux.Lock()
	defer catalogCallData.mux.Unlock()
	for _, e := range entries {
		catalogCallData.addedEntries[e.Path] = true
	}
	atomic.AddInt32(catalogCallData.callLog["CreateEntries"], 1)
	return nil
}
func (m mockCataloger) DeleteEntry(_ context.Context, _, _ string, path string) error {
	catalogCallData.mux.Lock()
	defer catalogCallData.mux.Unlock()
	catalogCallData.deletedEntries[path] = true
	atomic.AddInt32(catalogCallData.callLog["DeleteEntry"], 1)
	return nil
}

func TestCreateAndDeleteRows(t *testing.T) {
	c := onboard.NewCatalogActions(mockCataloger{}, "example-repo", "committer", logging.Default())
	c.(*onboard.CatalogRepoActions).WriteBatchSize = 5
	catalogActions, ok := c.(*onboard.CatalogRepoActions)
	if !ok {
		t.Fatal("NewCatalogActions return value implement doesn't match")
	}
	testdata := []struct {
		AddedRows           []string
		DeletedRows         []string
		ExpectedAddCalls    int32
		ExpectedDeleteCalls int32
	}{
		{
			AddedRows:           []string{"a1", "b2", "c3"},
			DeletedRows:         []string{},
			ExpectedAddCalls:    1,
			ExpectedDeleteCalls: 0,
		},
		{
			AddedRows:           []string{"a1", "b2", "c3"},
			DeletedRows:         []string{"d1", "e2"},
			ExpectedAddCalls:    1,
			ExpectedDeleteCalls: 2,
		},
		{
			AddedRows:           []string{"a1", "b2", "c3", "d4", "e5"},
			DeletedRows:         []string{"f1", "g2"},
			ExpectedAddCalls:    1,
			ExpectedDeleteCalls: 2,
		},
		{
			AddedRows:           []string{"a1", "b2", "c3", "d4", "e5", "f6"},
			DeletedRows:         []string{"g1", "h2"},
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
	for _, dryRun := range []bool{true, false} {
		for _, test := range testdata {
			catalogCallData.addedEntries = make(map[string]bool)
			catalogCallData.deletedEntries = make(map[string]bool)
			catalogCallData.callLog = make(map[string]*int32)
			catalogCallData.callLog["DeleteEntry"] = swag.Int32(0)
			catalogCallData.callLog["CreateEntries"] = swag.Int32(0)
			stats, err := catalogActions.ApplyImport(context.Background(), onboard.NewDiffIterator(
				&mockInventoryIterator{rows: rows(test.DeletedRows...)},
				&mockInventoryIterator{rows: rows(test.AddedRows...)}), dryRun)
			if err != nil {
				t.Fatalf("failed to create/delete objects: %v", err)
			}
			expectedAddCalls := test.ExpectedAddCalls
			expectedDeleteCalls := test.ExpectedDeleteCalls
			if dryRun {
				expectedAddCalls = 0
				expectedDeleteCalls = 0
			}
			if *catalogCallData.callLog["CreateEntries"] != expectedAddCalls {
				t.Fatalf("unexpected number of CreateEntries calls. expected=%d, got=%d", expectedAddCalls, *catalogCallData.callLog["CreateEntries"])
			}
			if *catalogCallData.callLog["DeleteEntry"] != expectedDeleteCalls {
				t.Fatalf("unexpected number of DeleteEntries calls. expected=%d, got=%d", expectedDeleteCalls, *catalogCallData.callLog["DeleteEntry"])
			}
			if stats.AddedOrChanged != len(test.AddedRows) {
				t.Fatalf("unexpected number of added entries in returned stats. expected=%d, got=%d", len(test.AddedRows), stats.AddedOrChanged)
			}
			if stats.Deleted != len(test.DeletedRows) {
				t.Fatalf("unexpected number of deleted entries in returned stats. expected=%d, got=%d", len(test.DeletedRows), stats.Deleted)
			}
			if dryRun {
				continue
			}
			if len(catalogCallData.addedEntries) != len(test.AddedRows) {
				t.Fatalf("unexpected number of added entries. expected=%d, got=%d", len(test.AddedRows), len(catalogCallData.addedEntries))
			}
			for _, path := range test.AddedRows {
				if _, ok := catalogCallData.addedEntries[path]; !ok {
					t.Fatalf("expected entry not added: %s", path)
				}
			}
			if len(catalogCallData.deletedEntries) != len(test.DeletedRows) {
				t.Fatalf("unexpected number of deleted entries. expected=%d, got=%d", len(test.DeletedRows), len(catalogCallData.deletedEntries))
			}
			for _, path := range test.DeletedRows {
				if _, ok := catalogCallData.deletedEntries[path]; !ok {
					t.Fatalf("expected entry not deleted: %s", path)
				}
			}
		}
	}
}
