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

type Adapter interface {
	InventoryGenerator
	WithContext(ctx context.Context) Adapter
	Put(obj ObjectPointer, sizeBytes int64, reader io.Reader, opts PutOpts) error
	Get(obj ObjectPointer, expectedSize int64) (io.ReadCloser, error)
	GetRange(obj ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error)
	GetProperties(obj ObjectPointer) (Properties, error)
	Remove(obj ObjectPointer) error
	CreateMultiPartUpload(obj ObjectPointer, r *http.Request, opts CreateMultiPartUploadOpts) (string, error)
	UploadPart(obj ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error)
	AbortMultiPartUpload(obj ObjectPointer, uploadId string) error
	CompleteMultiPartUpload(obj ObjectPointer, uploadId string, MultipartList *MultipartUploadCompletion) (*string, int64, error)
	// ValidateConfiguration validates an appropriate bucket
	// configuration and returns a validation error or nil.
	ValidateConfiguration(storageNamespace string) error
}

type UploadIdTranslator interface {
	SetUploadId(uploadId string) string
	TranslateUploadId(simulationId string) string
	RemoveUploadId(inputUploadId string)
}

// the uploadId translator is required to enable re-play of recorded requests (playback_test)
// the NoOp translator is the default for non-simulated runs. a playback translator is implemented in playback_test
type NoOpTranslator struct{}

func (d *NoOpTranslator) SetUploadId(uploadId string) string {
	return uploadId
}
func (d *NoOpTranslator) TranslateUploadId(uploadId string) string {
	return uploadId
}
func (d *NoOpTranslator) RemoveUploadId(_ string) {}
