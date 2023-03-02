package operations_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

type mockAdapter struct {
	totalSize        int64
	count            int
	lastBucket       string
	lastStorageClass *string
}

func (a *mockAdapter) GetPreSignedURL(_ context.Context, _ block.ObjectPointer, _ block.PreSignMode) (string, error) {
	return "", block.ErrOperationNotSupported
}

func newMockAdapter() *mockAdapter {
	adapter := mockAdapter{
		totalSize:        0,
		count:            0,
		lastStorageClass: nil,
	}
	return &adapter
}

func (a *mockAdapter) Put(_ context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, opts block.PutOpts) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	a.totalSize += int64(len(data))
	a.count++
	a.lastBucket = obj.StorageNamespace
	a.lastStorageClass = opts.StorageClass
	return nil
}

func (a *mockAdapter) Exists(_ context.Context, _ block.ObjectPointer) (bool, error) {
	return false, nil
}

func (a *mockAdapter) Get(_ context.Context, _ block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	return nil, nil
}

func (a *mockAdapter) GetWalker(_ *url.URL) (block.Walker, error) {
	return nil, nil
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

func (a *mockAdapter) CreateMultiPartUpload(_ context.Context, _ block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	panic("try to create multipart in mock adapter")
}

func (a *mockAdapter) UploadPart(_ context.Context, _ block.ObjectPointer, _ int64, _ io.Reader, _ string, _ int) (*block.UploadPartResponse, error) {
	panic("try to upload part in mock adapter")
}

func (a *mockAdapter) AbortMultiPartUpload(_ context.Context, _ block.ObjectPointer, _ string) error {
	panic("try to abort multipart in mock adapter")
}

func (a *mockAdapter) CompleteMultiPartUpload(_ context.Context, _ block.ObjectPointer, _ string, _ *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	panic("try to complete multipart in mock adapter")
}

func (a *mockAdapter) UploadCopyPart(_ context.Context, _, _ block.ObjectPointer, _ string, _ int) (*block.UploadPartResponse, error) {
	panic("try to upload copy part in mock adapter")
}

func (a *mockAdapter) UploadCopyPartRange(_ context.Context, _, _ block.ObjectPointer, _ string, _ int, _, _ int64) (*block.UploadPartResponse, error) {
	panic("try to upload copy part range in mock adapter")
}

func (a *mockAdapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, nil
}

func (a *mockAdapter) BlockstoreType() string {
	return "s3"
}

func (a *mockAdapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	info := block.DefaultStorageNamespaceInfo("mock")
	info.PreSignSupport = false
	info.ImportSupport = false
	return info
}

func (a *mockAdapter) ResolveNamespace(storageNamespace, key string, identifierType block.IdentifierType) (block.QK, error) {
	return block.ResolveNamespace(storageNamespace, key, identifierType)
}

func (a *mockAdapter) RuntimeStats() map[string]string {
	return nil
}
