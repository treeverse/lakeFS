package block

import "context"

type InventoryGenerator interface {
	GenerateInventory(ctx context.Context, inventoryURL string) (Inventory, error)
}

// Inventory represents a snapshot of the storage space
type Inventory interface {
	Iterator() InventoryIterator
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
