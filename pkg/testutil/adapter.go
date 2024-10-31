package testutil

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
)

type MockAdapter struct {
	TotalSize        int64
	Count            int
	LastBucket       string
	LastStorageClass *string

	blockstoreMetadata *block.BlockstoreMetadata
	namespaceRegion    *string
}

var (
	errorGetPropertiesNotImplemented = errors.New("getProperties method not implemented in mock adapter")
	errorRemoveNotImplemented        = errors.New("remove method not implemented in mock adapter")
	errorCopyNotImplemented          = errors.New("copy method not implemented in mock adapter")
)

type MockAdapterOption func(a *MockAdapter)

func NewMockAdapter(opts ...MockAdapterOption) *MockAdapter {
	adapter := &MockAdapter{
		TotalSize:        0,
		Count:            0,
		LastStorageClass: nil,
	}
	for _, opt := range opts {
		opt(adapter)
	}
	return adapter
}

func WithBlockstoreMetadata(bm *block.BlockstoreMetadata) func(a *MockAdapter) {
	return func(a *MockAdapter) {
		a.blockstoreMetadata = bm
	}
}

func WithNamespaceRegion(region string) func(a *MockAdapter) {
	return func(a *MockAdapter) {
		a.namespaceRegion = &region
	}
}

func (a *MockAdapter) GetPreSignedURL(_ context.Context, _ block.ObjectPointer, _ block.PreSignMode) (string, time.Time, error) {
	return "", time.Time{}, block.ErrOperationNotSupported
}

func (a *MockAdapter) GetPresignUploadPartURL(_ context.Context, _ block.ObjectPointer, _ string, _ int) (string, error) {
	return "", block.ErrOperationNotSupported
}

func (a *MockAdapter) Put(_ context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, opts block.PutOpts) (*block.PutResponse, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	a.TotalSize += int64(len(data))
	a.Count++
	a.LastBucket = obj.StorageNamespace
	a.LastStorageClass = opts.StorageClass
	return &block.PutResponse{}, nil
}

func (a *MockAdapter) Exists(_ context.Context, _ block.ObjectPointer) (bool, error) {
	return false, nil
}

func (a *MockAdapter) Get(_ context.Context, _ block.ObjectPointer) (io.ReadCloser, error) {
	return nil, nil
}

func (a *MockAdapter) GetWalker(_ *url.URL) (block.Walker, error) {
	return nil, nil
}

func (a *MockAdapter) GetRange(_ context.Context, _ block.ObjectPointer, _ int64, _ int64) (io.ReadCloser, error) {
	return nil, nil
}

func (a *MockAdapter) GetProperties(_ context.Context, _ block.ObjectPointer) (block.Properties, error) {
	return block.Properties{}, errorGetPropertiesNotImplemented
}

func (a *MockAdapter) Remove(_ context.Context, _ block.ObjectPointer) error {
	return errorRemoveNotImplemented
}

func (a *MockAdapter) Copy(_ context.Context, _, _ block.ObjectPointer) error {
	return errorCopyNotImplemented
}

func (a *MockAdapter) CreateMultiPartUpload(_ context.Context, _ block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	panic("try to create multipart in mock adapter")
}

func (a *MockAdapter) UploadPart(_ context.Context, _ block.ObjectPointer, _ int64, _ io.Reader, _ string, _ int) (*block.UploadPartResponse, error) {
	panic("try to upload part in mock adapter")
}

func (a *MockAdapter) AbortMultiPartUpload(_ context.Context, _ block.ObjectPointer, _ string) error {
	panic("try to abort multipart in mock adapter")
}

func (a *MockAdapter) CompleteMultiPartUpload(_ context.Context, _ block.ObjectPointer, _ string, _ *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	panic("try to complete multipart in mock adapter")
}

func (a *MockAdapter) UploadCopyPart(_ context.Context, _, _ block.ObjectPointer, _ string, _ int) (*block.UploadPartResponse, error) {
	panic("try to upload copy part in mock adapter")
}

func (a *MockAdapter) UploadCopyPartRange(_ context.Context, _, _ block.ObjectPointer, _ string, _ int, _, _ int64) (*block.UploadPartResponse, error) {
	panic("try to upload copy part range in mock adapter")
}

func (a *MockAdapter) ListParts(_ context.Context, _ block.ObjectPointer, _ string, _ block.ListPartsOpts) (*block.ListPartsResponse, error) {
	panic("try to list parts in mock adapter")
}

func (a *MockAdapter) BlockstoreType() string {
	return "s3"
}

func (a *MockAdapter) BlockstoreMetadata(_ context.Context) (*block.BlockstoreMetadata, error) {
	if a.blockstoreMetadata != nil {
		return a.blockstoreMetadata, nil
	} else {
		return nil, block.ErrOperationNotSupported
	}
}

func (a *MockAdapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	info := block.DefaultStorageNamespaceInfo("s3")
	info.PreSignSupport = false
	info.ImportSupport = false
	return info
}

func (a *MockAdapter) ResolveNamespace(storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	return block.DefaultResolveNamespace(storageNamespace, key, identifierType)
}

func (a *MockAdapter) GetRegion(_ context.Context, _ string) (string, error) {
	if a.namespaceRegion != nil {
		return *a.namespaceRegion, nil
	} else {
		return "", block.ErrOperationNotSupported
	}
}

func (a *MockAdapter) RuntimeStats() map[string]string {
	return nil
}
