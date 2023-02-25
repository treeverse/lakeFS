package block

import (
	"context"
	"io"
	"net/http"
	"time"
)

// MultipartPart single multipart information
type MultipartPart struct {
	ETag       string
	PartNumber int
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

// CompleteMultiPartUploadResponse complete multipart etag, content length and additional headers (implementation specific) currently it targets s3.
// The ETag is a hex string value of the content checksum
type CompleteMultiPartUploadResponse struct {
	ETag             string
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

// Properties of an object stored on the underlying block store.
// Refer to the actual underlying Adapter for which properties are
// actually reported.
type Properties struct {
	StorageClass *string
}

type Adapter interface {
	InventoryGenerator
	Put(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, opts PutOpts) error
	Get(ctx context.Context, obj ObjectPointer, expectedSize int64) (io.ReadCloser, error)
	GetPreSignedURL(ctx context.Context, obj ObjectPointer, mode PreSignMode) (string, error)
	Exists(ctx context.Context, obj ObjectPointer) (bool, error)
	GetRange(ctx context.Context, obj ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error)
	GetProperties(ctx context.Context, obj ObjectPointer) (Properties, error)
	Remove(ctx context.Context, obj ObjectPointer) error
	Copy(ctx context.Context, sourceObj, destinationObj ObjectPointer) error
	CreateMultiPartUpload(ctx context.Context, obj ObjectPointer, r *http.Request, opts CreateMultiPartUploadOpts) (*CreateMultiPartUploadResponse, error)
	UploadPart(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int) (*UploadPartResponse, error)
	UploadCopyPart(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int) (*UploadPartResponse, error)
	UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*UploadPartResponse, error)
	AbortMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string) error
	CompleteMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string, multipartList *MultipartUploadCompletion) (*CompleteMultiPartUploadResponse, error)
	BlockstoreType() string
	GetStorageNamespaceInfo() StorageNamespaceInfo
	RuntimeStats() map[string]string
}
