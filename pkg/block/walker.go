package block

import (
	"context"
	"fmt"
	"net/url"
	"time"
)

type Walker interface {
	Walk(ctx context.Context, storageURI *url.URL, op WalkOptions, walkFn func(e ObjectStoreEntry) error) error
	Marker() Mark
}

type ObjectStoreEntry struct {
	// FullKey represents the fully qualified path in the object store namespace for the given entry
	FullKey string `json:"full_key,omitempty"`
	// RelativeKey represents a path relative to prefix (or directory). If none specified, will be identical to FullKey
	RelativeKey string `json:"relative_key,omitempty"`
	// Address is a full URI for the entry, including the storage namespace (i.e. s3://bucket/path/to/key)
	Address string `json:"address,omitempty"`
	// ETag represents a hash of the entry's content. Generally as hex encoded MD5,
	// but depends on the underlying object store
	ETag string `json:"etag,omitempty"`
	// Mtime is the last-modified datetime of the entry
	Mtime time.Time `json:"mtime,omitempty"`
	// Size in bytes
	Size int64 `json:"size"`
}

type WalkOptions struct {
	// All walked items must be greater than After
	After string

	// ContinuationToken is passed to the client for efficient listing.
	// Value is Opaque to the caller.
	ContinuationToken string
}

type Mark struct {
	ContinuationToken string
	LastKey           string
	HasMore           bool
}

func (e ObjectStoreEntry) String() string {
	return fmt.Sprintf("ObjectStoreEntry: {Address:%s, RelativeKey:%s, ETag:%s, Size:%d, Mtime:%s}",
		e.Address, e.RelativeKey, e.ETag, e.Size, e.Mtime)
}
