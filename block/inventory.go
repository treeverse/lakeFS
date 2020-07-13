package block

import (
	"context"
)

type InventoryGenerator interface {
	GenerateInventory(inventoryURL string) (Inventory, error)
}

// Inventory represents a snapshot of the storage space
type Inventory interface {
	Objects(ctx context.Context, sorted bool) (objects []InventoryObject, err error)
	SourceName() string
	InventoryURL() string
}

type InventoryObject struct {
	Error           error
	Bucket          string `parquet:"name=bucket, type=INTERVAL"`
	Key             string `parquet:"name=key, type=INTERVAL"`
	Size            *int64 `parquet:"name=size, type=INT_64"`
	Checksum        string `parquet:"name=e_tag, type=INTERVAL"`
	LastModified    int64  `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS"`
	PhysicalAddress string
}
