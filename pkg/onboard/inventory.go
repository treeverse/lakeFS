package onboard

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/cmdutils"
)

type InventoryDiff struct {
	DryRun               bool
	AddedOrChanged       []block.InventoryObject
	Deleted              []block.InventoryObject
	PreviousInventoryURL string
	PreviousImportDate   time.Time
}
type ImportObject struct {
	Obj       block.InventoryObject
	IsDeleted bool
	IsChanged bool
}

type Iterator interface {
	cmdutils.ProgressReporter
	Next() bool
	Err() error
	Get() ImportObject
}

// onboard.InventoryIterator reads from block.InventoryIterator and converts the objects to ImportObject
type InventoryIterator struct {
	block.InventoryIterator
}

func NewInventoryIterator(it block.InventoryIterator) *InventoryIterator {
	return &InventoryIterator{InventoryIterator: it}
}

func (s *InventoryIterator) Get() ImportObject {
	return ImportObject{
		Obj:       *s.InventoryIterator.Get(),
		IsDeleted: false,
	}
}

func CreateCommitMetadata(inv block.Inventory, stats Stats, prefixes []string) catalog.Metadata {
	metadata := catalog.Metadata{
		"inventory_url":            inv.InventoryURL(),
		"source":                   inv.SourceName(),
		"added_or_changed_objects": strconv.Itoa(stats.AddedOrChanged),
	}
	if len(prefixes) > 0 {
		prefixesSerialized, _ := json.Marshal(prefixes)
		metadata["key_prefixes"] = string(prefixesSerialized)
	}
	return metadata
}
