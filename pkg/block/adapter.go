package block

import (
	"context"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go/service/s3"
)

type MultipartUploadCompletion struct{ Part []*s3.CompletedPart }

// ObjectPointer is a unique identifier of an object in the object
// store: the store is a 1:1 mapping between pointers and objects.
type ObjectPointer struct {
	StorageNamespace string
	Identifier       string
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

// CreateMultiPartOpts contains optional arguments for
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

// WalkFunc is called for each object visited by the Walk.
// The id argument contains the argument to Walk as a prefix; that is, if Walk is called with "test/data/",
// which is a prefix containing the object "test/data/a", the walk function will be called with argument "test/data/a".
// If there was a problem walking to the object, the incoming error will describe the problem and the function can decide
// how to handle that error.
// If an error is returned, processing stops.
type WalkFunc func(id string) error

type Adapter interface {
	InventoryGenerator
	Put(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, opts PutOpts) error
	Get(ctx context.Context, obj ObjectPointer, expectedSize int64) (io.ReadCloser, error)
	Walk(ctx context.Context, walkOpt WalkOpts, walkFn WalkFunc) error
	Exists(ctx context.Context, obj ObjectPointer) (bool, error)
	GetRange(ctx context.Context, obj ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error)
	GetProperties(ctx context.Context, obj ObjectPointer) (Properties, error)
	Remove(ctx context.Context, obj ObjectPointer) error
	Copy(ctx context.Context, sourceObj, destinationObj ObjectPointer) error
	CreateMultiPartUpload(ctx context.Context, obj ObjectPointer, r *http.Request, opts CreateMultiPartUploadOpts) (string, error)
	UploadPart(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error)
	UploadCopyPart(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int64) (string, error)
	UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber, startPosition, endPosition int64) (string, error)
	AbortMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string) error
	CompleteMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string, multipartList *MultipartUploadCompletion) (*string, int64, error)
	// ValidateConfiguration validates an appropriate bucket
	// configuration and returns a validation error or nil.
	ValidateConfiguration(ctx context.Context, storageNamespace string) error
	BlockstoreType() string
	GetStorageNamespaceInfo() StorageNamespaceInfo
}

type UploadIDTranslator interface {
	SetUploadID(uploadID string) string
	TranslateUploadID(simulationID string) string
	RemoveUploadID(inputUploadID string)
}

// the uploadID translator is required to enable re-play of recorded requests (playback_test)
// the NoOp translator is the default for non-simulated runs. a playback translator is implemented in playback_test
type NoOpTranslator struct{}

func (d *NoOpTranslator) SetUploadID(uploadID string) string {
	return uploadID
}
func (d *NoOpTranslator) TranslateUploadID(uploadID string) string {
	return uploadID
}
func (d *NoOpTranslator) RemoveUploadID(_ string) {}
