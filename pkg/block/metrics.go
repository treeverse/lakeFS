package block

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/treeverse/lakefs/pkg/httputil"
)

type MetricsAdapter struct {
	adapter Adapter
}

func NewMetricsAdapter(adapter Adapter) Adapter {
	return &MetricsAdapter{adapter: adapter}
}

func (m *MetricsAdapter) InnerAdapter() Adapter {
	return m.adapter
}

func (m *MetricsAdapter) Put(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, opts PutOpts) error {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Put(ctx, obj, sizeBytes, reader, opts)
}

func (m *MetricsAdapter) Get(ctx context.Context, obj ObjectPointer) (io.ReadCloser, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Get(ctx, obj)
}

func (m *MetricsAdapter) GetWalker(uri *url.URL) (Walker, error) {
	return m.adapter.GetWalker(uri)
}

func (m *MetricsAdapter) GetPreSignedURL(ctx context.Context, obj ObjectPointer, mode PreSignMode) (string, time.Time, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetPreSignedURL(ctx, obj, mode)
}

func (m *MetricsAdapter) GetPresignUploadPartURL(ctx context.Context, obj ObjectPointer, uploadID string, partNumber int) (string, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetPresignUploadPartURL(ctx, obj, uploadID, partNumber)
}

func (m *MetricsAdapter) Exists(ctx context.Context, obj ObjectPointer) (bool, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Exists(ctx, obj)
}

func (m *MetricsAdapter) GetRange(ctx context.Context, obj ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetRange(ctx, obj, startPosition, endPosition)
}

func (m *MetricsAdapter) GetProperties(ctx context.Context, obj ObjectPointer) (Properties, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetProperties(ctx, obj)
}

func (m *MetricsAdapter) Remove(ctx context.Context, obj ObjectPointer) error {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Remove(ctx, obj)
}

func (m *MetricsAdapter) Copy(ctx context.Context, sourceObj, destinationObj ObjectPointer) error {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.Copy(ctx, sourceObj, destinationObj)
}

func (m *MetricsAdapter) CreateMultiPartUpload(ctx context.Context, obj ObjectPointer, r *http.Request, opts CreateMultiPartUploadOpts) (*CreateMultiPartUploadResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.CreateMultiPartUpload(ctx, obj, r, opts)
}

func (m *MetricsAdapter) UploadPart(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int) (*UploadPartResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.UploadPart(ctx, obj, sizeBytes, reader, uploadID, partNumber)
}

func (m *MetricsAdapter) ListParts(ctx context.Context, obj ObjectPointer, uploadID string, opts ListPartsOpts) (*ListPartsResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.ListParts(ctx, obj, uploadID, opts)
}

func (m *MetricsAdapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int) (*UploadPartResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.UploadCopyPart(ctx, sourceObj, destinationObj, uploadID, partNumber)
}

func (m *MetricsAdapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*UploadPartResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.UploadCopyPartRange(ctx, sourceObj, destinationObj, uploadID, partNumber, startPosition, endPosition)
}

func (m *MetricsAdapter) AbortMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string) error {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.AbortMultiPartUpload(ctx, obj, uploadID)
}

func (m *MetricsAdapter) CompleteMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string, multipartList *MultipartUploadCompletion) (*CompleteMultiPartUploadResponse, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.CompleteMultiPartUpload(ctx, obj, uploadID, multipartList)
}

func (m *MetricsAdapter) BlockstoreType() string {
	return m.adapter.BlockstoreType()
}

func (m *MetricsAdapter) BlockstoreMetadata(ctx context.Context) (*BlockstoreMetadata, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.BlockstoreMetadata(ctx)
}

func (m *MetricsAdapter) GetStorageNamespaceInfo() StorageNamespaceInfo {
	return m.adapter.GetStorageNamespaceInfo()
}

func (m *MetricsAdapter) ResolveNamespace(storageNamespace, key string, identifierType IdentifierType) (QualifiedKey, error) {
	return m.adapter.ResolveNamespace(storageNamespace, key, identifierType)
}

func (m *MetricsAdapter) GetRegion(ctx context.Context, storageNamespace string) (string, error) {
	ctx = httputil.SetClientTrace(ctx, m.adapter.BlockstoreType())
	return m.adapter.GetRegion(ctx, storageNamespace)
}

func (m *MetricsAdapter) RuntimeStats() map[string]string {
	return m.adapter.RuntimeStats()
}
