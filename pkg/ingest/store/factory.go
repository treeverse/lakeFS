package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/block/params"
)

var ErrNotSupported = errors.New("no storage adapter found")

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

func NewWrapper(walker Walker, uri *url.URL) *WalkerWrapper {
	return &WalkerWrapper{
		walker: walker,
		uri:    uri,
	}
}

func (ww *WalkerWrapper) Walk(ctx context.Context, opts WalkOptions, walkFn func(e ObjectStoreEntry) error) error {
	return ww.walker.Walk(ctx, ww.uri, opts, walkFn)
}

func (ww *WalkerWrapper) Marker() Mark {
	return ww.walker.Marker()
}

type walkerFactory struct {
	params params.AdapterConfig
}

func NewFactory(params params.AdapterConfig) *walkerFactory {
	return &walkerFactory{params: params}
}

func (f *walkerFactory) buildS3Walker(opts WalkerOptions) (*s3Walker, error) {
	var sess *session.Session
	if f.params != nil {
		s3params, err := f.params.BlockstoreS3Params()
		if err != nil {
			return nil, err
		}
		sess, err = factory.BuildS3Client(s3params.AwsConfig, s3params.SkipVerifyCertificateTestOnly)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		sess, err = getS3Client(opts.S3EndpointURL)
		if err != nil {
			return nil, err
		}
	}
	return NewS3Walker(sess), nil
}

func (f *walkerFactory) buildGCSWalker(ctx context.Context) (*gcsWalker, error) {
	var svc *storage.Client
	if f.params != nil {
		gsParams, err := f.params.BlockstoreGSParams()
		if err != nil {
			return nil, err
		}
		svc, err = factory.BuildGSClient(ctx, gsParams)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		svc, err = storage.NewClient(ctx)
		if err != nil {
			return nil, err
		}
	}
	return NewGCSWalker(svc), nil
}

func (f *walkerFactory) buildAzureWalker() (*azureBlobWalker, error) {
	var p pipeline.Pipeline
	if f.params != nil {
		azureParams, err := f.params.BlockstoreAzureParams()
		if err != nil {
			return nil, err
		}
		p, err = factory.BuildAzureClient(azureParams)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		p, err = getAzureClient()
		if err != nil {
			return nil, err
		}
	}
	return NewAzureBlobWalker(p)
}

func (f *walkerFactory) GetWalker(ctx context.Context, opts WalkerOptions) (*WalkerWrapper, error) {
	var walker Walker
	uri, err := url.Parse(opts.StorageURI)
	if err != nil {
		return nil, fmt.Errorf("could not parse storage URI %s: %w", uri, err)
	}
	switch uri.Scheme {
	case "s3":
		walker, err = f.buildS3Walker(opts)
		if err != nil {
			return nil, fmt.Errorf("creating s3 walker: %w", err)
		}
	case "gs":
		walker, err = f.buildGCSWalker(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating gs walker: %w", err)
		}
	case "http", "https":
		walker, err = f.buildAzureWalker()
		if err != nil {
			return nil, fmt.Errorf("creating Azure walker: %w", err)
		}
	default:
		return nil, fmt.Errorf("%w: for scheme: %s", ErrNotSupported, uri.Scheme)
	}

	return NewWrapper(walker, uri), nil
}
