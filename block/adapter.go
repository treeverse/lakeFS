package block

import (
	"context"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"net/http"
)

type MultipartUploadCompletion struct{ Part []*s3.CompletedPart }

// ObjectPointer is a unique identifier of an object in the object
// store: the store is a 1:1 mapping between pointers and objects.
type ObjectPointer struct {
	Repo       string
	Identifier string
}

type Adapter interface {
	WithContext(ctx context.Context) Adapter
	Put(obj ObjectPointer, sizeBytes int64, reader io.Reader) error
	Get(obj ObjectPointer) (io.ReadCloser, error)
	GetRange(obj ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error)
	Remove(obj ObjectPointer) error
	CreateMultiPartUpload(obj ObjectPointer, r *http.Request) (string, error)
	UploadPart(obj ObjectPointer, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (string, error)
	AbortMultiPartUpload(obj ObjectPointer, uploadId string) error
	CompleteMultiPartUpload(obj ObjectPointer, uploadId string, MultipartList *MultipartUploadCompletion) (*string, int64, error)
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
func (d *NoOpTranslator) RemoveUploadId(_ string) {
	return
}
