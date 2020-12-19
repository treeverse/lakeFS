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
		PreviousInventory            []string
		ExpectedAdded                []string
		ExpectedDeleted              []string
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
			ExpectedAdded: []string{"f2", "f1"},
		},
		"with previous inventory": {
			NewInventory:      []string{"f1", "f2", "f3", "f4"},
			PreviousInventory: []string{"f1", "f2"},
			ExpectedAdded:     []string{"f3", "f4"},
			ExpectedDeleted:   nil,
		},
		"with previous inventory - unsorted": {
			NewInventory:      []string{"f4", "f3", "f2", "f1"},
			PreviousInventory: []string{"f1", "f2"},
			ExpectedAdded:     []string{"f3", "f4"},
			ExpectedDeleted:   nil,
		},
		"delete objects": {
			NewInventory:      []string{"f1", "f2"},
			PreviousInventory: []string{"f1", "f2", "f3", "f4"},
			ExpectedAdded:     nil,
			ExpectedDeleted:   []string{"f3", "f4"},
		},
		"add and delete objects": {
			NewInventory:      []string{"f1", "f2", "s1"},
			PreviousInventory: []string{"f1", "f2", "f3", "f4"},
			ExpectedAdded:     []string{"s1"},
			ExpectedDeleted:   []string{"f3", "f4"},
		},
		"delete all objects": {
			NewInventory:      []string{},
			PreviousInventory: []string{"f1", "f2", "f3", "f4"},
			ExpectedAdded:     nil,
			ExpectedDeleted:   []string{"f1", "f2", "f3", "f4"},
		},
		"do nothing": {
			// do nothing, expect no errors
		},
		"complex add and delete": {
			NewInventory:      []string{"a01", "a02", "a03", "a04", "a05", "a06", "a07"},
			PreviousInventory: []string{"a01", "a02", "a04", "a08", "a09", "a10"},
			ExpectedDeleted:   []string{"a08", "a09", "a10"},
			ExpectedAdded:     []string{"a03", "a05", "a06", "a07"},
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
		"prefix incompatible with previous": {
			NewInventory:      []string{"a1", "a2", "b1", "b2", "c1", "c2"},
			PreviousInventory: []string{"a1", "a2", "b1", "b2"},
			Prefixes:          []string{"a", "c"},
			PreviousPrefixes:  []string{"a"},
			ExpectedErr:       onboard.ErrIncompatiblePrefixes,
		},
		"import with prefix - with previous import": {
			NewInventory:      []string{"a1", "a2", "b1", "b2", "c1", "c2"},
			PreviousInventory: []string{"a1", "a2", "a3", "b1", "b2", "b3"},
			Prefixes:          []string{"a"},
			PreviousPrefixes:  []string{"a"},
			ExpectedDeleted:   []string{"a3"},
		},
		"prefix when previous import was without prefixes": {
			NewInventory:      []string{"a1", "a2", "b1", "b2", "c1", "c2"},
			PreviousInventory: []string{"a1", "a2", "a3", "b1", "b2", "b3"},
			PreviousPrefixes:  []string{"a"},
			ExpectedErr:       onboard.ErrIncompatiblePrefixes,
		},
	}
	for _, dryRun := range []bool{true, false} {
		for name, test := range testdata {
			t.Run(name, func(t *testing.T) {
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
						previousCommitPrefixes:  test.PreviousPrefixes,
					}
				}
				inventoryGenerator := &mockInventoryGenerator{
					newInventoryURL:      newInventoryURL,
					previousInventoryURL: previousInventoryURL,
					newInventory:         test.NewInventory,
					previousInventory:    test.PreviousInventory,
					sourceBucket:         "example-repo",
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
					return
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
			})
		}

	}
}
