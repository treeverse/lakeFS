package operations_test

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
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

func (a *mockAdapter) Put(ctx context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, opts block.PutOpts) error {
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

func (a *mockAdapter) Exists(context.Context, block.ObjectPointer) (bool, error) {
	return false, nil
}

func (a *mockAdapter) Get(_ context.Context, obj block.ObjectPointer, expectedSize int64) (io.ReadCloser, error) {
	return nil, nil
}

func (a *mockAdapter) Walk(_ context.Context, _ block.WalkOpts, _ block.WalkFunc) error {
	return nil
}

func (a *mockAdapter) GetRange(_ context.Context, _ block.ObjectPointer, _ int64, _ int64) (io.ReadCloser, error) {
	return nil, nil
}

func (a *mockAdapter) GetProperties(_ context.Context, _ block.ObjectPointer) (block.Properties, error) {
	return block.Properties{}, errors.New("getProperties method not implemented in mock adapter")
}

func (a *mockAdapter) Remove(_ context.Context, _ block.ObjectPointer) error {
	return errors.New("remove method not implemented in mock adapter")
}
func (a *mockAdapter) Copy(_ context.Context, _, _ block.ObjectPointer) error {
	return errors.New("copy method not implemented in mock adapter")
}
func (a *mockAdapter) CreateMultiPartUpload(_ context.Context, _ block.ObjectPointer, r *http.Request, _ block.CreateMultiPartUploadOpts) (string, error) {
	panic("try to create multipart in mock adapter")
}

func (a *mockAdapter) UploadPart(_ context.Context, _ block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	panic("try to upload part in mock adapter")
}

func (a *mockAdapter) AbortMultiPartUpload(_ context.Context, _ block.ObjectPointer, uploadID string) error {
	panic("try to abort multipart in mock adapter")
}

func (a *mockAdapter) CompleteMultiPartUpload(_ context.Context, _ block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	panic("try to complete multipart in mock adapter")
}

func (a *mockAdapter) UploadCopyPart(_ context.Context, _, _ block.ObjectPointer, _ string, _ int64) (string, error) {
	panic("try to upload copy part in mock adapter")
}

func (a *mockAdapter) UploadCopyPartRange(_ context.Context, _, _ block.ObjectPointer, _ string, _, _, _ int64) (string, error) {
	panic("try to upload copy part range in mock adapter")
}

func (a *mockAdapter) ValidateConfiguration(_ context.Context, _ string) error {
	return nil
}

func (a *mockAdapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, nil
}

func (a *mockAdapter) BlockstoreType() string {
	return "s3"
}

func (a *mockAdapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	panic("try to get storage namespace info")
}
