package block

import "context"

type InventoryGenerator interface {
	GenerateInventory(inventoryURL string) (Inventory, error)
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

type EmptyInventoryIterator struct {
}

func (e EmptyInventoryIterator) Next() bool {
	return false
}

func (e EmptyInventoryIterator) Err() error {
	return nil
}

func (e EmptyInventoryIterator) Get() *InventoryObject {
	return nil
}
