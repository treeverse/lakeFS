package operations_test

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
)

type mockAdapter struct {
	totalSize        int64
	count            int
	lastBucket       string
	lastStorageClass *string
}

func newMockAdapter() *mockAdapter {
	adapter := mockAdapter{
		totalSize:        0,
		count:            0,
		lastStorageClass: nil,
	}
	return &adapter
}

func (a *mockAdapter) WithContext(_ context.Context) block.Adapter {
	return a
}

func (a *mockAdapter) Put(obj block.ObjectPointer, _ int64, reader io.Reader, opts block.PutOpts) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	a.totalSize += int64(len(data))
	a.count++
	a.lastBucket = obj.StorageNamespace
	a.lastStorageClass = opts.StorageClass
	return nil
}

func (a *mockAdapter) Get(obj block.ObjectPointer, expectedSize int64) (io.ReadCloser, error) {
	return nil, nil
}

func (a *mockAdapter) GetRange(_ block.ObjectPointer, _ int64, _ int64) (io.ReadCloser, error) {
	return nil, nil
}

func (a *mockAdapter) GetProperties(_ block.ObjectPointer) (block.Properties, error) {
	return block.Properties{}, errors.New("getProperties method not implemented in mock adapter")
}

func (a *mockAdapter) Remove(_ block.ObjectPointer) error {
	return errors.New("remove method not implemented in mock adapter")
}
func (a *mockAdapter) Copy(_, _ block.ObjectPointer) error {
	return errors.New("copy method not implemented in mock adapter")
}
func (a *mockAdapter) CreateMultiPartUpload(_ block.ObjectPointer, r *http.Request, _ block.CreateMultiPartUploadOpts) (string, error) {
	panic("try to create multipart in mock adapter")
}

func (a *mockAdapter) UploadPart(_ block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	panic("try to upload part in mock adapter")
}

func (a *mockAdapter) AbortMultiPartUpload(_ block.ObjectPointer, uploadID string) error {
	panic("try to abort multipart in mock adapter")

}

func (a *mockAdapter) CompleteMultiPartUpload(_ block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	panic("try to complete multipart in mock adapter")
}

func (a *mockAdapter) ValidateConfiguration(_ string) error {
	return nil
}

func (a *mockAdapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, nil
}

func (a *mockAdapter) BlockstoreType() string {
	return "s3"
}
