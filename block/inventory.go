package block

import (
	"context"

	"github.com/treeverse/lakefs/logging"
)

type InventoryGenerator interface {
	GenerateInventory(logger logging.Logger, inventoryURL string) (Inventory, error)
}

// Inventory represents a snapshot of the storage space
type Inventory interface {
	Iterator(ctx context.Context) (InventoryIterator, error)
	SourceName() string
	InventoryURL() string
}

type InventoryObject struct {
	Bucket          string
	Key             string
	Size            int64
	LastModified    int64
	Checksum        string
	PhysicalAddress string
}

type InventoryIterator interface {
	Next() bool
	Err() error
	Get() *InventoryObject
}
