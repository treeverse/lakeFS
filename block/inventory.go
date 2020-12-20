package block

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/logging"
)

type InventoryGenerator interface {
	GenerateInventory(ctx context.Context, logger logging.Logger, inventoryURL string, shouldSort bool, prefixes []string) (Inventory, error)
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
	LastModified    *time.Time
	Checksum        string
	PhysicalAddress string
}

type InventoryIterator interface {
	cmdutils.ProgressReporter
	Next() bool
	Err() error
	Get() *InventoryObject
}
