package onboard_test

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/logging"
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

func (m mockInventoryGenerator) GenerateInventory(_ context.Context, _ logging.Logger, inventoryURL string, _ bool) (block.Inventory, error) {
	if inventoryURL == m.newInventoryURL {
		return &mockInventory{rows: m.newInventory, inventoryURL: inventoryURL, sourceBucket: m.sourceBucket}, nil
	}
	if inventoryURL == m.previousInventoryURL {
		return &mockInventory{rows: m.previousInventory, inventoryURL: inventoryURL, sourceBucket: m.sourceBucket}, nil
	}
	return nil, errors.New("failed to create inventory")
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

func (m *mockCatalogActions) ApplyImport(_ context.Context, it onboard.Iterator, dryRun bool) (*onboard.InventoryImportStats, error) {
	stats := onboard.InventoryImportStats{
		AddedOrChanged: len(m.objectActions.Added),
		Deleted:        len(m.objectActions.Deleted),
	}
	for it.Next() {
		diffObj := it.Get()
		if diffObj.IsDeleted {
			if !dryRun {
				m.objectActions.Deleted = append(m.objectActions.Deleted, diffObj.Obj.Key)
			}
			stats.Deleted += 1
		} else {
			if !dryRun {
				m.objectActions.Added = append(m.objectActions.Added, diffObj.Obj.Key)
			}
			stats.AddedOrChanged += 1
		}
	}
	return &stats, nil
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

type mockInventoryIterator struct {
	idx  *int
	rows []block.InventoryObject
}

func (m *mockInventoryIterator) Next() bool {
	if m.idx == nil {
		m.idx = new(int)
	} else {
		*m.idx++
	}
	return *m.idx < len(m.rows)
}

func (m *mockInventoryIterator) Err() error {
	return nil
}

func (m *mockInventoryIterator) Get() *block.InventoryObject {
	return &m.rows[*m.idx]
}

func (m *mockInventory) Iterator() block.InventoryIterator {
	return &mockInventoryIterator{
		rows: rows(m.rows...),
	}
}

func (m *mockInventory) SourceName() string {
	return m.sourceBucket
}

func (m *mockInventory) InventoryURL() string {
	return m.inventoryURL
}
