package onboard_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
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
	prefixes     []string
}

type objectActions struct {
	Added   []string
	Deleted []string
}

type mockCatalogActions struct {
	objectActions      objectActions
	lastCommitMetadata catalog.Metadata
}

type mockInventoryGenerator struct {
	inventoryURL string
	inventory    []string
	sourceBucket string
}

func (m mockInventoryGenerator) GenerateInventory(_ context.Context, _ logging.Logger, inventoryURL string, shouldSort bool, prefixes []string) (block.Inventory, error) {
	if inventoryURL == m.inventoryURL {
		return &mockInventory{keys: m.inventory, inventoryURL: inventoryURL, sourceBucket: m.sourceBucket, shouldSort: shouldSort, prefixes: prefixes}, nil
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
	sort.Strings(m.prefixes)
	currentPrefix := 0
	for i, key := range m.keys {
		if len(m.prefixes) > 0 {
			for currentPrefix < len(m.prefixes) && m.prefixes[currentPrefix] < key && !strings.HasPrefix(key, m.prefixes[currentPrefix]) {
				currentPrefix++
			}
			if currentPrefix == len(m.prefixes) {
				break
			}
			if !strings.HasPrefix(key, m.prefixes[currentPrefix]) {
				continue
			}
		}
		res = append(res, block.InventoryObject{Key: key, LastModified: swag.Time(m.lastModified[i%len(m.lastModified)]), Checksum: m.checksum(key)})
	}
	return res
}

func (m *mockCatalogActions) ApplyImport(_ context.Context, it onboard.Iterator, dryRun bool) (*onboard.Stats, error) {
	stats := onboard.Stats{
		AddedOrChanged: len(m.objectActions.Added),
	}
	for it.Next() {
		diffObj := it.Get()
		if diffObj.IsDeleted {
			if !dryRun {
				m.objectActions.Deleted = append(m.objectActions.Deleted, diffObj.Obj.Key)
			}
		} else {
			if !dryRun {
				m.objectActions.Added = append(m.objectActions.Added, diffObj.Obj.Key)
			}
			stats.AddedOrChanged += 1
		}
	}
	return &stats, nil
}

func (m *mockCatalogActions) InitBranch(_ context.Context) error {
	return nil
}

func (m *mockCatalogActions) Commit(_ context.Context, _ string, metadata catalog.Metadata) (string, error) {
	m.lastCommitMetadata = metadata
	return "", nil
}

func (m *mockCatalogActions) Progress() []*cmdutils.Progress {
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

func (m *mockInventoryIterator) Progress() []*cmdutils.Progress {
	return nil
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
