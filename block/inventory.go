package block

import (
	"context"
)

type InventoryGenerator interface {
	GenerateInventory(inventoryURL string) (Inventory, error)
}

// Inventory represents a snapshot of the storage space
type Inventory interface {
	Objects(ctx context.Context, sorted bool) ([]InventoryObject, error)
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
