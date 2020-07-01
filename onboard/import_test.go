package onboard

import (
	"context"
	"reflect"
	"strconv"
	"testing"
)

func TestImport(t *testing.T) {
	testdata := []struct {
		NewInventory                []string
		PreviousInventory           []string
		ExpectedAdded               []string
		ExpectedDeleted             []string
		ExpectedErr                 bool
		OverrideNewManifestURL      string
		OverridePreviousManifestURL string
	}{
		{
			NewInventory:  []string{"f1", "f2"},
			ExpectedAdded: []string{"f1", "f2"},
		},
		{
			NewInventory:      []string{"f1", "f2"},
			PreviousInventory: []string{"f1", "f2"},
		},
		{
			NewInventory:      []string{"f1", "f2", "f4", "f3"},
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
			PreviousInventory: []string{"f4", "f3", "f2", "f1"},
			ExpectedAdded:     []string{"s1"},
			ExpectedDeleted:   []string{"f3", "f4"},
		},
		{
			NewInventory:      []string{},
			PreviousInventory: []string{"f4", "f3", "f2", "f1"},
			ExpectedAdded:     nil,
			ExpectedDeleted:   []string{"f1", "f2", "f3", "f4"},
		},
		{
			NewInventory:           []string{"f1", "f2", "f3", "f4"},
			PreviousInventory:      []string{"f1", "f2", "f3", "f4"},
			OverrideNewManifestURL: "s3://non-existing.json",
			ExpectedErr:            true,
		},
		{
			NewInventory:                []string{"f1", "f2", "f3", "f4"},
			PreviousInventory:           []string{"f1", "f2", "f3", "f4"},
			OverridePreviousManifestURL: "s3://non-existing.json",
			ExpectedErr:                 true,
		},
		{
			// do nothing, expect no errors
		},
		{
			OverridePreviousManifestURL: "s3://non-existing.json",
			ExpectedErr:                 true,
		},
		{
			NewInventory:      []string{"a1", "a2", "a3", "a4", "a7", "a6", "a5"},
			PreviousInventory: []string{"a9", "a10", "a2", "a4", "a1", "a8"},
			ExpectedDeleted:   []string{"a10", "a8", "a9"},
			ExpectedAdded:     []string{"a3", "a5", "a6", "a7"},
		},
		{
			// do not sort when no need to delete
			NewInventory:  []string{"a1", "a2", "a3", "a4", "a7", "a6", "a5"},
			ExpectedAdded: []string{"a1", "a2", "a3", "a4", "a7", "a6", "a5"},
		},
	}
	for _, test := range testdata {
		newManifestURL := NewManifestURL
		previousManifestURL := PreviousManifestURL
		if test.OverrideNewManifestURL != "" {
			newManifestURL = test.OverrideNewManifestURL
		}
		if test.OverridePreviousManifestURL != "" {
			newManifestURL = test.OverridePreviousManifestURL
		}
		importer := &Importer{s3: &mockS3Client{}, repository: "example-repo"}
		catalogActionsMock := mockCatalogActions{}
		if len(test.PreviousInventory) > 0 {
			catalogActionsMock = mockCatalogActions{
				previousCommitManifest: previousManifestURL,
			}
		}
		importer.catalogActions = &catalogActionsMock
		importer.inventoryCreator = getInventoryCreator(newManifestURL, previousManifestURL, test.NewInventory, test.PreviousInventory)
		importer.inventory = &mockInventory{manifest: &Manifest{URL: newManifestURL}, rows: test.NewInventory}
		importer.inventoryDiffer = getSimpleDiffer(t)
		err := importer.Import(context.Background())

		if !test.ExpectedErr && err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if test.ExpectedErr && err == nil {
			t.Fatalf("error was expected but none was returned")
		}
		if test.ExpectedErr {
			continue // error was expected, no need to check further
		}
		if !reflect.DeepEqual(catalogActionsMock.objectActions.Added, test.ExpectedAdded) {
			t.Fatalf("added objects different than expected. expected=%v, got=%v.", test.ExpectedAdded, catalogActionsMock.objectActions.Added)
		}
		if !reflect.DeepEqual(catalogActionsMock.objectActions.Deleted, test.ExpectedDeleted) {
			t.Fatalf("deleted objects different than expected. expected=%v, got=%v.", test.ExpectedDeleted, catalogActionsMock.objectActions.Deleted)
		}
		if catalogActionsMock.lastCommitMetadata["manifest_url"] != newManifestURL {
			t.Fatalf("unexpected manifest_url in commit metadata. expected=%s, got=%s", newManifestURL, catalogActionsMock.lastCommitMetadata["manifest_url"])
		}
		addedOrChangedCount, err := strconv.Atoi(catalogActionsMock.lastCommitMetadata["added_or_changed_objects"])
		if err != nil || addedOrChangedCount != len(test.ExpectedAdded) {
			t.Fatalf("unexpected added_or_changed_objects in commit metadata. expected=%d, got=%s", len(test.ExpectedAdded), catalogActionsMock.lastCommitMetadata["added_or_changed_objects"])
		}
		deletedCount, err := strconv.Atoi(catalogActionsMock.lastCommitMetadata["deleted_objects"])
		if err != nil || deletedCount != len(test.ExpectedDeleted) {
			t.Fatalf("unexpected deleted_objects in commit metadata. expected=%d, got=%s", len(test.ExpectedDeleted), catalogActionsMock.lastCommitMetadata["deleted_objects"])
		}
	}
}
