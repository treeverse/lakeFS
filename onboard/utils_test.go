package onboard_test

import (
	"context"
	"errors"
	"sort"
	"time"

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
	keys         []string
	inventoryURL string
	sourceBucket string
	shouldSort   bool
	lastModified []time.Time
	checksum     func(string) string
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

func (m mockInventoryGenerator) GenerateInventory(_ context.Context, _ logging.Logger, inventoryURL string, shouldSort bool) (block.Inventory, error) {
	if inventoryURL == m.newInventoryURL {
		return &mockInventory{keys: m.newInventory, inventoryURL: inventoryURL, sourceBucket: m.sourceBucket, shouldSort: shouldSort}, nil
	}
	if inventoryURL == m.previousInventoryURL {
		return &mockInventory{keys: m.previousInventory, inventoryURL: inventoryURL, sourceBucket: m.sourceBucket, shouldSort: shouldSort}, nil
	}
	return nil, errors.New("failed to create inventory")
}

func (m *mockInventory) rows() []block.InventoryObject {
	if m.keys == nil {
		return nil
	}
	res := make([]block.InventoryObject, 0, len(m.keys))
	if m.checksum == nil {
		m.checksum = func(s string) string { return s }
	}
	for i, key := range m.keys {

		res = append(res, block.InventoryObject{Key: key, LastModified: m.lastModified[i%len(m.lastModified)], Checksum: m.checksum(key)})
	}
	return res
}

func (m *mockCatalogActions) ApplyImport(_ context.Context, it onboard.Iterator, dryRun bool, stats *onboard.InventoryImportStats) error {
	for it.Next() {
		diffObj := it.Get()
		if diffObj.IsDeleted {
			if !dryRun {
				m.objectActions.Deleted = append(m.objectActions.Deleted, diffObj.Obj.Key)
			}
			*stats.Deleted += 1
		} else {
			if !dryRun {
				m.objectActions.Added = append(m.objectActions.Added, diffObj.Obj.Key)
			}
			*stats.AddedOrChanged += 1
		}
	}
	return nil
}

func (m *mockCatalogActions) GetPreviousCommit(_ context.Context) (commit *catalog.CommitLog, err error) {
	if m.previousCommitInventory != "" {
		return &catalog.CommitLog{Metadata: catalog.Metadata{"inventory_url": m.previousCommitInventory}}, nil
	}
	return nil, nil
}

func (m *mockCatalogActions) Commit(_ context.Context, _ string, metadata catalog.Metadata) (*catalog.CommitLog, error) {
	m.lastCommitMetadata = metadata
	return &catalog.CommitLog{}, nil
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
	if m.shouldSort {
		sort.Strings(m.keys)
	}
	if m.lastModified == nil {
		m.lastModified = []time.Time{time.Now()}
	}
	return &mockInventoryIterator{
		rows: m.rows(),
	}
}

func (m *mockInventory) SourceName() string {
	return m.sourceBucket
}

func (m *mockInventory) InventoryURL() string {
	return m.inventoryURL
}
