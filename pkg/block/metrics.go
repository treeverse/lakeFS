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

func (m *MetricsAdapter) Put(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, opts PutOpts) (*PutResponse, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.Put(ctx, obj, sizeBytes, reader, opts)
}

func (m *MetricsAdapter) Get(ctx context.Context, obj ObjectPointer) (io.ReadCloser, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.Get(ctx, obj)
}

func (m *MetricsAdapter) GetWalker(uri *url.URL) (Walker, error) {
	return m.adapter.GetWalker(uri)
}

func (m *MetricsAdapter) GetPreSignedURL(ctx context.Context, obj ObjectPointer, mode PreSignMode) (string, time.Time, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return "", time.Time{}, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.GetPreSignedURL(ctx, obj, mode)
}

func (m *MetricsAdapter) GetPresignUploadPartURL(ctx context.Context, obj ObjectPointer, uploadID string, partNumber int) (string, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return "", err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.GetPresignUploadPartURL(ctx, obj, uploadID, partNumber)
}

func (m *MetricsAdapter) Exists(ctx context.Context, obj ObjectPointer) (bool, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return false, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.Exists(ctx, obj)
}

func (m *MetricsAdapter) GetRange(ctx context.Context, obj ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.GetRange(ctx, obj, startPosition, endPosition)
}

func (m *MetricsAdapter) GetProperties(ctx context.Context, obj ObjectPointer) (Properties, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return Properties{}, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.GetProperties(ctx, obj)
}

func (m *MetricsAdapter) Remove(ctx context.Context, obj ObjectPointer) error {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.Remove(ctx, obj)
}

func (m *MetricsAdapter) Copy(ctx context.Context, sourceObj, destinationObj ObjectPointer) error {
	blockstoreType, err := m.adapter.BlockstoreType(sourceObj.StorageID)
	if err != nil {
		return err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.Copy(ctx, sourceObj, destinationObj)
}

func (m *MetricsAdapter) CreateMultiPartUpload(ctx context.Context, obj ObjectPointer, r *http.Request, opts CreateMultiPartUploadOpts) (*CreateMultiPartUploadResponse, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.CreateMultiPartUpload(ctx, obj, r, opts)
}

func (m *MetricsAdapter) UploadPart(ctx context.Context, obj ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int) (*UploadPartResponse, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.UploadPart(ctx, obj, sizeBytes, reader, uploadID, partNumber)
}

func (m *MetricsAdapter) ListParts(ctx context.Context, obj ObjectPointer, uploadID string, opts ListPartsOpts) (*ListPartsResponse, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.ListParts(ctx, obj, uploadID, opts)
}

func (m *MetricsAdapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int) (*UploadPartResponse, error) {
	blockstoreType, err := m.adapter.BlockstoreType(sourceObj.StorageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.UploadCopyPart(ctx, sourceObj, destinationObj, uploadID, partNumber)
}

func (m *MetricsAdapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*UploadPartResponse, error) {
	blockstoreType, err := m.adapter.BlockstoreType(sourceObj.StorageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.UploadCopyPartRange(ctx, sourceObj, destinationObj, uploadID, partNumber, startPosition, endPosition)
}

func (m *MetricsAdapter) AbortMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string) error {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.AbortMultiPartUpload(ctx, obj, uploadID)
}

func (m *MetricsAdapter) CompleteMultiPartUpload(ctx context.Context, obj ObjectPointer, uploadID string, multipartList *MultipartUploadCompletion) (*CompleteMultiPartUploadResponse, error) {
	blockstoreType, err := m.adapter.BlockstoreType(obj.StorageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.CompleteMultiPartUpload(ctx, obj, uploadID, multipartList)
}

func (m *MetricsAdapter) BlockstoreType(storageID string) (string, error) {
	return m.adapter.BlockstoreType(storageID)
}

func (m *MetricsAdapter) BlockstoreMetadata(ctx context.Context, storageID string) (*BlockstoreMetadata, error) {
	blockstoreType, err := m.adapter.BlockstoreType(storageID)
	if err != nil {
		return nil, err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.BlockstoreMetadata(ctx, storageID)
}

func (m *MetricsAdapter) GetStorageNamespaceInfo(storageID string) (StorageNamespaceInfo, error) {
	return m.adapter.GetStorageNamespaceInfo(storageID)
}

func (m *MetricsAdapter) ResolveNamespace(storageID, storageNamespace, key string, identifierType IdentifierType) (QualifiedKey, error) {
	return m.adapter.ResolveNamespace(storageID, storageNamespace, key, identifierType)
}

func (m *MetricsAdapter) GetRegion(ctx context.Context, storageID, storageNamespace string) (string, error) {
	blockstoreType, err := m.adapter.BlockstoreType(storageID)
	if err != nil {
		return "", err
	}
	ctx = httputil.SetClientTrace(ctx, blockstoreType)
	return m.adapter.GetRegion(ctx, storageID, storageNamespace)
}

func (m *MetricsAdapter) RuntimeStats() map[string]string {
	return m.adapter.RuntimeStats()
}
