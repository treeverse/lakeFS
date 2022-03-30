package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"
)

var (
	ErrNoStorageAdapter = errors.New("no storage adapter found")
)

type ObjectStoreEntry struct {
	// FullKey represents the fully qualified path in the object store namespace for the given entry
	FullKey string
	// RelativeKey represents a path relative to prefix (or directory). If none specified, will be identical to FullKey
	RelativeKey string
	// Address is a full URI for the entry, including the storage namespace (i.e. s3://bucket/path/to/key)
	Address string
	// ETag represents a hash of the entry's content. Generally as hex encoded MD5,
	// but depends on the underlying object store
	ETag string
	// Mtime is the last-modified datetime of the entry
	Mtime time.Time
	// Size in bytes
	Size int64
}

type Walker interface {
	Walk(ctx context.Context, storageURI *url.URL, walkFn func(e ObjectStoreEntry) error) error
}

func (e ObjectStoreEntry) String() string {
	return fmt.Sprintf("ObjectStoreEntry: {Address:%s, RelativeKey:%s, ETag:%s, Size:%d, Mtime:%s}",
		e.Address, e.RelativeKey, e.ETag, e.Size, e.Mtime)
}

func Walk(ctx context.Context, s3EndpointURL string, storageURI string, walkFn func(e ObjectStoreEntry) error) error {
	var walker Walker
	uri, err := url.Parse(storageURI)
	if err != nil {
		return fmt.Errorf("could not parse storage URI %s: %w", uri, err)
	}
	switch uri.Scheme {
	case "s3":
		svc, err := GetS3Client(s3EndpointURL)
		if err != nil {
			return err
		}
		walker = &S3Walker{s3: svc}
	case "gs":
		svc, err := GetGCSClient(ctx)
		if err != nil {
			return err
		}
		walker = &GCSWalker{client: svc}
	case "http", "https":
		svc, err := GetAzureClient()
		if err != nil {
			return err
		}
		walker = &AzureBlobWalker{client: svc}
	default:
		return fmt.Errorf("%w: for scheme: %s", ErrNoStorageAdapter, uri.Scheme)
	}
	return walker.Walk(ctx, uri, walkFn)
}
