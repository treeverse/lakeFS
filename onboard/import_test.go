package onboard_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
)

func TestImport(t *testing.T) {
	testdata := map[string]struct {
		NewInventory                 []string
		ExpectedAdded                []string
		ExpectedErr                  error
		OverrideNewInventoryURL      string
		OverridePreviousInventoryURL string
		Prefixes                     []string
		PreviousPrefixes             []string
	}{
		"new inventory": {
			NewInventory:  []string{"f1", "f2"},
			ExpectedAdded: []string{"f1", "f2"},
		},
		"new inventory - unsorted": {
			NewInventory:  []string{"f2", "f1"},
			ExpectedAdded: []string{"f1", "f2"},
		},
		"do nothing": {
			// do nothing, expect no errors
		},
		"add many objects": {
			NewInventory:  []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"},
			ExpectedAdded: []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"},
		},
		"import with prefix - prefix at start": {
			NewInventory:  []string{"a1", "a2", "b1", "b2"},
			Prefixes:      []string{"a"},
			ExpectedAdded: []string{"a1", "a2"},
		},
		"import with prefix - multiple prefixes": {
			NewInventory:  []string{"a1", "a2", "b1", "b2", "c1", "c2", "d1", "d2"},
			Prefixes:      []string{"b", "d"},
			ExpectedAdded: []string{"b1", "b2", "d1", "d2"},
		},
		"import with prefix - prefixes unsorted": {
			NewInventory:  []string{"a1", "a2", "b1", "b2", "c1", "c2", "d1", "d2"},
			Prefixes:      []string{"d", "b"},
			ExpectedAdded: []string{"b1", "b2", "d1", "d2"},
		},
		"import with prefix - prefix at end": {
			NewInventory:  []string{"a1", "a2", "b1", "b2"},
			Prefixes:      []string{"b"},
			ExpectedAdded: []string{"b1", "b2"},
		},
	}
	for _, dryRun := range []bool{true, false} {
		for name, test := range testdata {
			t.Run(name, func(t *testing.T) {
				newInventoryURL := NewInventoryURL
				if test.OverrideNewInventoryURL != "" {
					newInventoryURL = test.OverrideNewInventoryURL
				}
				if test.OverridePreviousInventoryURL != "" {
					newInventoryURL = test.OverridePreviousInventoryURL
				}
				catalogActionsMock := mockCatalogActions{}
				inventoryGenerator := &mockInventoryGenerator{
					inventoryURL: newInventoryURL,
					inventory:    test.NewInventory,
					sourceBucket: "example-repo",
				}
				config := &onboard.Config{
					CommitUsername:     "committer",
					InventoryURL:       newInventoryURL,
					Repository:         "example-repo",
					InventoryGenerator: inventoryGenerator,
					CatalogActions:     &catalogActionsMock,
					KeyPrefixes:        test.Prefixes,
				}
				importer, err := onboard.CreateImporter(context.TODO(), logging.Default(), config)
				if err != nil {
					if !errors.Is(err, test.ExpectedErr) {
						t.Fatalf("unexpected error: %v", err)
					} else {
						return
					}
				}
				if test.ExpectedErr != nil {
					t.Fatalf("error was expected but none was returned")
				}
				stats, err := importer.Import(context.Background(), dryRun)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !reflect.DeepEqual(stats.AddedOrChanged, len(test.ExpectedAdded)) {
					t.Fatalf("number of added objects in return value different than expected. expected=%v, got=%v", len(test.ExpectedAdded), stats.AddedOrChanged)
				}
				var expectedAddedToCatalog, expectedDeletedFromCatalog []string
				if !dryRun {
					expectedAddedToCatalog = test.ExpectedAdded
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
					return
				}
				if catalogActionsMock.lastCommitMetadata["inventory_url"] != newInventoryURL {
					t.Fatalf("unexpected inventory_url in commit metadata. expected=%s, got=%s", newInventoryURL, catalogActionsMock.lastCommitMetadata["inventory_url"])
				}

				addedOrChangedCount, err := strconv.Atoi(catalogActionsMock.lastCommitMetadata["added_or_changed_objects"])
				if err != nil || addedOrChangedCount != len(expectedAddedToCatalog) {
					t.Fatalf("unexpected added_or_changed_objects in commit metadata. expected=%d, got=%d", len(expectedDeletedFromCatalog), addedOrChangedCount)
				}
			})
		}

	}
}
