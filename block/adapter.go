package block

import (
	"context"
	"io"
	"net/http"
)

type Adapter interface {
	WithContext(ctx context.Context) Adapter
	Put(repo string, identifier string, sizeBytes int64, reader io.Reader) error
	Get(repo string, identifier string) (io.ReadCloser, error)
	GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error)
	Remove(repo string, identifier string) error
	GetAdapterType() string
	CreateMultiPartUpload(repo string, identifier string, r *http.Request) (string, error)
	UploadPart(repo string, identifier string, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (string, error)
	AbortMultiPartUpload(repo string, identifier string, uploadId string) error
	CompleteMultiPartUpload(repo string, identifier string, uploadId string, XMLmultiPartComplete []byte) (*string, int64, error)
	InjectSimulationId(u UploadIdTranslator)
}

type UploadIdTranslator interface {
	SetUploadId(uploadId string) string
	TranslateUploadId(smulationId string) string
	RemoveUploadId(inputUploadId string)
}

type DummyTranslator struct{}

func (d *DummyTranslator) SetUploadId(uploadId string) string {
	return uploadId
}
func (d *DummyTranslator) TranslateUploadId(smulationId string) string {
	return smulationId
}
func (d *DummyTranslator) RemoveUploadId(inputUploadId string) {
	return
}
