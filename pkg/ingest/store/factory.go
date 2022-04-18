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

type WalkOptions struct {
	// All walked items must be greater then After
	After string

	// ContinuationToken is passed to the client for efficient listing.
	// Value is Opaque to the caller.
	ContinuationToken string
}

type Mark struct {
	ContinuationToken string
	HasMore           bool
}

type Walker interface {
	Walk(ctx context.Context, storageURI *url.URL, op WalkOptions, walkFn func(e ObjectStoreEntry) error) error
	Marker() Mark
}

func (e ObjectStoreEntry) String() string {
	return fmt.Sprintf("ObjectStoreEntry: {Address:%s, RelativeKey:%s, ETag:%s, Size:%d, Mtime:%s}",
		e.Address, e.RelativeKey, e.ETag, e.Size, e.Mtime)
}

type WalkerOptions struct {
	S3EndpointURL string
	StorageURI    string
}

type WalkerWrapper struct {
	walker Walker
	uri    *url.URL
}

func (ww *WalkerWrapper) Walk(ctx context.Context, opts WalkOptions, walkFn func(e ObjectStoreEntry) error) error {
	return ww.walker.Walk(ctx, ww.uri, opts, walkFn)
}

func (ww *WalkerWrapper) Marker() Mark {
	return ww.walker.Marker()
}

func GetWalker(ctx context.Context, opts WalkerOptions) (*WalkerWrapper, error) {
	var walker Walker
	uri, err := url.Parse(opts.StorageURI)
	if err != nil {
		return nil, fmt.Errorf("could not parse storage URI %s: %w", uri, err)
	}
	switch uri.Scheme {
	case "s3":
		svc, err := GetS3Client(opts.S3EndpointURL)
		if err != nil {
			return nil, err
		}
		walker = &S3Walker{s3: svc, hasMore: true}
	case "gs":
		svc, err := GetGCSClient(ctx)
		if err != nil {
			return nil, err
		}
		walker = &GCSWalker{client: svc}
	case "http", "https":
		walker, err = NewAzureBlobWalker()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: for scheme: %s", ErrNoStorageAdapter, uri.Scheme)
	}

	return &WalkerWrapper{
		walker: walker,
		uri:    uri,
	}, nil
}
