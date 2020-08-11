package onboard_test

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
)

func TestImport(t *testing.T) {
	testdata := []struct {
		NewInventory                 []string
		PreviousInventory            []string
		ExpectedAdded                []string
		ExpectedDeleted              []string
		ExpectedErr                  bool
		OverrideNewInventoryURL      string
		OverridePreviousInventoryURL string
	}{
		{
			NewInventory:  []string{"f1", "f2"},
			ExpectedAdded: []string{"f1", "f2"},
		},
		{
			NewInventory:      []string{"f1", "f2", "f3", "f4"},
			PreviousInventory: []string{"f1", "f2"},
			ExpectedAdded:     []string{"f3", "f4"},
			ExpectedDeleted:   nil,
		},
		{
			NewInventory:      []string{"f1", "f2"},
			PreviousInventory: []string{"f1", "f2", "f3", "f4"},
			ExpectedAdded:     nil,
			ExpectedDeleted:   []string{"f3", "f4"},
		},
		{
			NewInventory:      []string{"f1", "f2", "s1"},
			PreviousInventory: []string{"f1", "f2", "f3", "f4"},
			ExpectedAdded:     []string{"s1"},
			ExpectedDeleted:   []string{"f3", "f4"},
		},
		{
			NewInventory:      []string{},
			PreviousInventory: []string{"f1", "f2", "f3", "f4"},
			ExpectedAdded:     nil,
			ExpectedDeleted:   []string{"f1", "f2", "f3", "f4"},
		},
		{
			// do nothing, expect no errors
		},
		{
			NewInventory:      []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"},
			PreviousInventory: []string{"a1", "a2", "a4", "a8", "a9", "a10"},
			ExpectedDeleted:   []string{"a8", "a9", "a10"},
			ExpectedAdded:     []string{"a3", "a5", "a6", "a7"},
		},
		{
			NewInventory:  []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"},
			ExpectedAdded: []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"},
		},
	}
	for _, dryRun := range []bool{true, false} {
		for _, test := range testdata {
			newInventoryURL := NewInventoryURL
			previousInventoryURL := PreviousInventoryURL
			if test.OverrideNewInventoryURL != "" {
				newInventoryURL = test.OverrideNewInventoryURL
			}
			if test.OverridePreviousInventoryURL != "" {
				newInventoryURL = test.OverridePreviousInventoryURL
			}
			catalogActionsMock := mockCatalogActions{}
			if len(test.PreviousInventory) > 0 {
				catalogActionsMock = mockCatalogActions{
					previousCommitInventory: previousInventoryURL,
				}
			}
			importer, err := onboard.CreateImporter(context.TODO(), logging.Default(), nil, &mockInventoryGenerator{
				newInventoryURL:      newInventoryURL,
				previousInventoryURL: previousInventoryURL,
				newInventory:         test.NewInventory,
				previousInventory:    test.PreviousInventory,
				sourceBucket:         "example-repo",
			}, "committer", newInventoryURL, "example-repo")
			if err != nil {
				t.Fatalf("failed to create importer: %v", err)
			}
			importer.CatalogActions = &catalogActionsMock
			stats, err := importer.Import(context.Background(), dryRun)
			if err != nil {
				if !test.ExpectedErr {
					t.Fatalf("unexpected error: %v", err)
				} else {
					continue
				}
			}
			if test.ExpectedErr {
				t.Fatalf("error was expected but none was returned")
			}

			if !reflect.DeepEqual(stats.AddedOrChanged, len(test.ExpectedAdded)) {
				t.Fatalf("number of added objects in return value different than expected. expected=%v, got=%v", len(test.ExpectedAdded), stats.AddedOrChanged)
			}
			if !reflect.DeepEqual(stats.Deleted, len(test.ExpectedDeleted)) {
				t.Fatalf("number of deleted objects in return value different than expected. expected=%v, got=%v", len(test.ExpectedDeleted), stats.Deleted)
			}
			var expectedAddedToCatalog, expectedDeletedFromCatalog []string
			if !dryRun {
				expectedAddedToCatalog = test.ExpectedAdded
				expectedDeletedFromCatalog = test.ExpectedDeleted
			}
			if !reflect.DeepEqual(catalogActionsMock.objectActions.Added, expectedAddedToCatalog) {
				t.Fatalf("objects added to catalog different than expected. expected=%v, got=%v.", expectedAddedToCatalog, catalogActionsMock.objectActions.Added)
			}
			if !reflect.DeepEqual(catalogActionsMock.objectActions.Deleted, expectedDeletedFromCatalog) {
				t.Fatalf("objects deleted from catalog different than expected. expected=%v, got=%v.", expectedDeletedFromCatalog, catalogActionsMock.objectActions.Deleted)
			}
			if stats.DryRun != dryRun {
				t.Fatalf("dryRun boolean on return value different than expected, expected=%t, got=%t", dryRun, stats.DryRun)
			}
			if dryRun {
				if len(catalogActionsMock.lastCommitMetadata) > 0 {
					t.Fatalf("found commit metadata in dry run: %v", catalogActionsMock.lastCommitMetadata)
				}
				continue
			}
			if catalogActionsMock.lastCommitMetadata["inventory_url"] != newInventoryURL {
				t.Fatalf("unexpected inventory_url in commit metadata. expected=%s, got=%s", newInventoryURL, catalogActionsMock.lastCommitMetadata["inventory_url"])
			}

			addedOrChangedCount, err := strconv.Atoi(catalogActionsMock.lastCommitMetadata["added_or_changed_objects"])
			if err != nil || addedOrChangedCount != len(expectedAddedToCatalog) {
				t.Fatalf("unexpected added_or_changed_objects in commit metadata. expected=%d, got=%d", len(expectedDeletedFromCatalog), addedOrChangedCount)
			}
			deletedCount, err := strconv.Atoi(catalogActionsMock.lastCommitMetadata["deleted_objects"])
			if err != nil || deletedCount != len(expectedDeletedFromCatalog) {
				t.Fatalf("unexpected deleted_objects in commit metadata. expected=%d, got=%d", len(expectedDeletedFromCatalog), deletedCount)
			}
		}
	}
}
