package block

import (
	"context"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"net/http"
)

type Adapter interface {
	WithContext(ctx context.Context) Adapter
	Put(repo string, identifier string, sizeBytes int64, reader io.Reader) error
	Get(repo string, identifier string) (io.ReadCloser, error)
	GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error)
	Remove(repo string, identifier string) error
	CreateMultiPartUpload(repo string, identifier string, r *http.Request) (string, error)
	UploadPart(repo string, identifier string, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (string, error)
	AbortMultiPartUpload(repo string, identifier string, uploadId string) error
	CompleteMultiPartUpload(repo string, identifier string, uploadId string, MultipartList *struct{ Parts []*s3.CompletedPart }) (*string, int64, error)
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
