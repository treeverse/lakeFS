package block

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// MultipartPart single multipart information
type MultipartPart struct {
	ETag         string
	PartNumber   int
	LastModified time.Time
	Size         int64
}

// MultipartUploadCompletion parts described as part of complete multipart upload. Each part holds the part number and ETag received while calling part upload.
// NOTE that S3 implementation and our S3 gateway accept and returns ETag value surrounded with double-quotes ("), while
// the adapter implementations supply the raw value of the etag (without double quotes) and let the gateway manage the s3
// protocol specifications.
type MultipartUploadCompletion struct {
	Part []MultipartPart
}

// IdentifierType is the type the ObjectPointer Identifier
type IdentifierType int32

// PreSignMode is the mode to use when generating a pre-signed URL (read/write)
type PreSignMode int32

const (
	BlockstoreTypeS3        = "s3"
	BlockstoreTypeGS        = "gs"
	BlockstoreTypeAzure     = "azure"
	BlockstoreTypeLocal     = "local"
	BlockstoreTypeMem       = "mem"
	BlockstoreTypeTransient = "transient"
)

const (
	// Deprecated: indicates that the identifier might be relative or full.
	IdentifierTypeUnknownDeprecated IdentifierType = 0

	// IdentifierTypeRelative indicates that the address is relative to the storage namespace.
	// For example: "/foo/bar"
	IdentifierTypeRelative IdentifierType = 1

	// IdentifierTypeFull indicates that the address is the full address of the object in the object store.
	// For example: "s3://bucket/foo/bar"
	IdentifierTypeFull IdentifierType = 2
)

const (
	PreSignModeRead PreSignMode = iota
	PreSignModeWrite
)

// DefaultPreSignExpiryDuration is the amount of time pre-signed requests are valid for.
const DefaultPreSignExpiryDuration = 15 * time.Minute

// ObjectPointer is a unique identifier of an object in the object
// store: the store is a 1:1 mapping between pointers and objects.
type ObjectPointer struct {
	StorageID        string
	StorageNamespace string
	Identifier       string

	// Indicates whether the Identifier is relative to the StorageNamespace,
	// full address to an object, or unknown.
	IdentifierType IdentifierType
}

// PutOpts contains optional arguments for Put.  These should be
// analogous to options on some underlying storage layer.  Missing
// arguments are mapped to the default if a storage layer implements
// the option.
//
// If the same Put command is implemented multiple times with the same
// contents but different option values, the first supplied option
// value is retained.
type PutOpts struct {
	StorageClass *string // S3 storage class
}

// WalkOpts is a unique identifier of a prefix in the object store.
type WalkOpts struct {
	StorageNamespace string
	Prefix           string
}

// CreateMultiPartUploadResponse multipart upload ID and additional headers (implementation specific) currently it targets s3
// capabilities to enable encryption properties
type CreateMultiPartUploadResponse struct {
	UploadID         string
	ServerSideHeader http.Header
}

// CompleteMultiPartUploadResponse complete multipart etag, content length and additional headers (implementation specific).
type CompleteMultiPartUploadResponse struct {
	// ETag is a hex string value of the content checksum
	ETag string
	// MTime, if non-nil, is the creation time of the resulting object.  Typically the
	// object store returns it on a Last-Modified header from some operations.
	MTime            *time.Time
	ContentLength    int64
	ServerSideHeader http.Header
}

// UploadPartResponse upload part ETag and additional headers (implementation specific) currently it targets s3
// capabilities to enable encryption properties
// The ETag is a hex string value of the content checksum
type UploadPartResponse struct {
	ETag             string
	ServerSideHeader http.Header
}

type ListPartsResponse struct {
	Parts                []MultipartPart
	NextPartNumberMarker *string
	IsTruncated          bool
}

type ListMultipartUploadsResponse struct {
	Uploads            []types.MultipartUpload
	NextUploadIDMarker *string
	NextKeyMarker      *string
	IsTruncated        bool
	MaxUploads         *int32
}

// CreateMultiPartUploadOpts contains optional arguments for
// CreateMultiPartUpload.  These should be analogous to options on
// some underlying storage layer.  Missing arguments are mapped to the
// default if a storage layer implements the option.
//
// If the same CreateMultiPartUpload command is implemented multiple times with the same
// contents but different option values, the first supplied option
// value is retained.
type CreateMultiPartUploadOpts struct {
	StorageClass *string // S3 storage class
}

// ListPartsOpts contains optional arguments for the ListParts request.
type ListPartsOpts struct {
	MaxParts         *int32
	PartNumberMarker *string
}

type ListMultipartUploadsOpts struct {
	MaxUploads     *int32
	UploadIDMarker *string
	KeyMarker      *string
}

// Properties of an object stored on the underlying block store.
// Refer to the actual underlying Adapter for which properties are
// actually reported.
type Properties struct {
	StorageClass *string
}

type BlockstoreMetadata struct {
	Region *string
}

type PutResponse struct {
	ModTime *time.Time
}

func (r *PutResponse) GetMtime() time.Time {
	if r != nil && r.ModTime != nil {
		return *r.ModTime
	}
	return time.Now()
}

// Adapter abstract Storage Adapter for persistence of version controlled data. The methods generally map to S3 API methods
// - Generally some type of Object Storage
// - Can also be block storage or even in-memory
type Adapter interface {
	Put(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, opts PutOpts) (*PutResponse, error)
	Get(ctx context.Context, obj ObjectPointer) (io.ReadCloser, error)

	GetWalker(storageID string, opts WalkerOptions) (Walker, error)

	// GetPreSignedURL returns a pre-signed URL for accessing obj with mode, and the
	// expiry time for this URL.  The expiry time IsZero() if reporting
	// expiry is not supported.  The expiry time will be sooner than
	// Config.*.PreSignedExpiry if an auth token is about to expire.
	// If filename is not empty, it will be used as the content-disposition filename
	// when downloading the object (if the underlying storage supports it).
	GetPreSignedURL(ctx context.Context, obj ObjectPointer, mode PreSignMode, filename string) (string, time.Time, error)
	GetPresignUploadPartURL(ctx context.Context, obj ObjectPointer, uploadID string, partNumber int) (string, error)

	Exists(ctx context.Context, obj ObjectPointer) (bool, error)
	GetRange(ctx context.Context, obj ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error)
	GetProperties(ctx context.Context, obj ObjectPointer) (Properties, error)
	Remove(ctx context.Context, obj ObjectPointer) error
	Copy(ctx context.Context, sourceObj, destinationObj ObjectPointer) error

	CreateMultiPartUpload(ctx context.Context, obj ObjectPointer, r *http.Request, opts CreateMultiPartUploadOpts) (*CreateMultiPartUploadResponse, error)
	UploadPart(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int) (*UploadPartResponse, error)
	UploadCopyPart(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int) (*UploadPartResponse, error)
	UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*UploadPartResponse, error)
	ListParts(ctx context.Context, obj ObjectPointer, uploadID string, opts ListPartsOpts) (*ListPartsResponse, error)
	ListMultipartUploads(ctx context.Context, obj ObjectPointer, opts ListMultipartUploadsOpts) (*ListMultipartUploadsResponse, error)
	AbortMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string) error
	CompleteMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string, multipartList *MultipartUploadCompletion) (*CompleteMultiPartUploadResponse, error)

	BlockstoreType() string
	BlockstoreMetadata(ctx context.Context) (*BlockstoreMetadata, error)
	// GetStorageNamespaceInfo returns the StorageNamespaceInfo for storageID or nil if not found.
	GetStorageNamespaceInfo(storageID string) *StorageNamespaceInfo
	ResolveNamespace(storageID, storageNamespace, key string, identifierType IdentifierType) (QualifiedKey, error)

	// GetRegion storageID is not actively used, and it's here mainly for completeness
	GetRegion(ctx context.Context, storageID, storageNamespace string) (string, error)

	RuntimeStats() map[string]string
}

type WalkerOptions struct {
	StorageURI *url.URL
	// SkipOutOfOrder skips non-lexically ordered entries (Azure only).
	SkipOutOfOrder bool
}

type WalkerWrapper struct {
	walker Walker
	uri    *url.URL
}

func NewWalkerWrapper(walker Walker, uri *url.URL) *WalkerWrapper {
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

func (ww *WalkerWrapper) GetSkippedEntries() []ObjectStoreEntry {
	return ww.walker.GetSkippedEntries()
}
