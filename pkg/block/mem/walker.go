package mem

import (
	"context"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
)

// Walker implements the block.Walker interface for the mem adapter
type Walker struct {
	adapter   *Adapter
	storageID string
	mark      block.Mark
}

// NewMemWalker creates a new Walker for the mem adapter
func NewMemWalker(storageID string, adapter *Adapter) *Walker {
	return &Walker{
		adapter:   adapter,
		storageID: storageID,
		mark:      block.Mark{HasMore: true},
	}
}

// Walk walks the objects in the mem adapter's data structure
func (w *Walker) Walk(_ context.Context, storageURI *url.URL, op block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	w.adapter.mutex.RLock()
	defer w.adapter.mutex.RUnlock()

	// Extract the prefix from the storageURI
	const schemePrefix = block.BlockstoreTypeMem + "://"
	prefix := schemePrefix + storageURI.Host + "/" + strings.TrimLeft(storageURI.Path, "/")

	// basePath is the path relative to which the walk is done
	var basePath string
	if idx := strings.LastIndex(prefix, "/"); idx != -1 {
		basePath = prefix[:idx+1]
	}

	// Get all keys that match the prefix, but only for the relevant storageID
	dataMap, ok := w.adapter.data[w.storageID]
	if !ok {
		dataMap = nil
	}
	var keys []string
	for key := range dataMap {
		if strings.HasPrefix(key, prefix) && key > op.After {
			keys = append(keys, key)
		}
	}

	// Sort keys to ensure a consistent order
	sort.Strings(keys)

	// Walk through the keys
	for _, key := range keys {
		data := dataMap[key]

		// Use current time as modification time
		mtime := time.Now()
		fullKey := strings.TrimPrefix(key, schemePrefix)
		relativeKey := strings.TrimPrefix(key, basePath)
		etag := calcETag(data)

		// Create the entry
		ent := block.ObjectStoreEntry{
			FullKey:     fullKey,
			RelativeKey: relativeKey,
			Address:     key,
			ETag:        etag,
			Mtime:       mtime,
			Size:        int64(len(data)),
		}
		if err := walkFn(ent); err != nil {
			return err
		}
	}

	// Mark that we're done
	w.mark = block.Mark{
		LastKey: "",
		HasMore: false,
	}

	return nil
}

// Marker returns the current marker
func (w *Walker) Marker() block.Mark {
	return w.mark
}

// GetSkippedEntries returns any skipped entries
func (w *Walker) GetSkippedEntries() []block.ObjectStoreEntry {
	return nil
}
