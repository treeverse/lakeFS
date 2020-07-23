package onboard_test

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/onboard"
)

const (
	NewInventoryURL      = "s3://example-bucket/inventory-new.json"
	PreviousInventoryURL = "s3://example-bucket/inventory-prev.json"
)

type mockInventory struct {
	rows         []string
	inventoryURL string
	sourceBucket string
}

type objectActions struct {
	Added   []string
	Deleted []string
}

type mockCatalogActions struct {
	onboard.RepoActions
	previousCommitInventory string
	objectActions           objectActions
	lastCommitMetadata      catalog.Metadata
}

type mockInventoryGenerator struct {
	newInventoryURL      string
	previousInventoryURL string
	newInventory         []string
	previousInventory    []string
	sourceBucket         string
}

func (m mockInventoryGenerator) GenerateInventory(inventoryURL string) (block.Inventory, error) {
	if inventoryURL == m.newInventoryURL {
		return &mockInventory{rows: m.newInventory, inventoryURL: inventoryURL, sourceBucket: m.sourceBucket}, nil
	}
	if inventoryURL == m.previousInventoryURL {
		return &mockInventory{rows: m.previousInventory, inventoryURL: inventoryURL, sourceBucket: m.sourceBucket}, nil
	}
	return nil, errors.New("failed to create inventory")
}

// convenience converter functions
func keys(rows []block.InventoryObject) []string {
	if rows == nil {
		return nil
	}
	res := make([]string, 0, len(rows))
	for _, row := range rows {
		res = append(res, row.Key)
	}
	return res
}

func rows(keys ...string) []block.InventoryObject {
	if keys == nil {
		return nil
	}
	res := make([]block.InventoryObject, 0, len(keys))
	for _, key := range keys {
		res = append(res, block.InventoryObject{Key: key})
	}
	return res
}

func (m *mockCatalogActions) CreateAndDeleteObjects(_ context.Context, objects []block.InventoryObject, objectsToDelete []block.InventoryObject) (err error) {
	m.objectActions.Added = append(m.objectActions.Added, keys(objects)...)
	m.objectActions.Deleted = append(m.objectActions.Deleted, keys(objectsToDelete)...)
	return nil
}

func (m *mockCatalogActions) GetPreviousCommit(_ context.Context) (commit *catalog.CommitLog, err error) {
	if m.previousCommitInventory != "" {
		return &catalog.CommitLog{Metadata: catalog.Metadata{"inventory_url": m.previousCommitInventory}}, nil
	}
	return nil, nil
}

func (m *mockCatalogActions) Commit(_ context.Context, _ string, metadata catalog.Metadata) error {
	m.lastCommitMetadata = metadata
	return nil
}

func (m *mockInventory) Objects(_ context.Context, sorted bool) ([]block.InventoryObject, error) {
	if sorted {
		sort.Strings(m.rows)
	}
	return rows(m.rows...), nil
}

func (m *mockInventory) SourceName() string {
	return m.sourceBucket
}

func (m *mockInventory) InventoryURL() string {
	return m.inventoryURL
}

func (m *mockInventory) CreateCommitMetadata(diff onboard.InventoryDiff) catalog.Metadata {
	return catalog.Metadata{
		"inventory_url":            m.inventoryURL,
		"source_bucket":            m.sourceBucket,
		"added_or_changed_objects": strconv.Itoa(len(diff.AddedOrChanged)),
		"deleted_objects":          strconv.Itoa(len(diff.Deleted)),
	}
}

func getSimpleDiffer(t *testing.T) func(leftInv []block.InventoryObject, rightInv []block.InventoryObject) *onboard.InventoryDiff {
	return func(leftInv []block.InventoryObject, rightInv []block.InventoryObject) *onboard.InventoryDiff {
		if !sort.StringsAreSorted(keys(leftInv)) || !sort.StringsAreSorted(keys(rightInv)) {
			t.Fatalf("inventory expected to be sorted at this point")
		}
		// inefficient diff
		diff := onboard.InventoryDiff{}
		for _, o1 := range leftInv {
			found := false
			for _, o2 := range rightInv {
				if o1.Key == o2.Key {
					found = true
					break
				}
			}
			if !found {
				diff.Deleted = append(diff.Deleted, o1)
			}
		}
		for _, o2 := range rightInv {
			found := false
			for _, o1 := range leftInv {
				if o1.Key == o2.Key {
					found = true
					break
				}
			}
			if !found {
				diff.AddedOrChanged = append(diff.AddedOrChanged, o2)
			}
		}
		return &diff
	}
}
