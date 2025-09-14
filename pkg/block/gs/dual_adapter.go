package gs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
)

const defaultLakeFSPrefix = "_lakefs/"

var ErrCrossAdapterCopy = fmt.Errorf("cross adapter copy is not supported: %w", block.ErrOperationNotSupported)

// DualAdapter routes operations between metadata and data adapters based on object path
type DualAdapter struct {
	metadataAdapter block.Adapter
	dataAdapter     block.Adapter
}

// NewDualAdapter creates a new dual adapter that routes operations based on object path
func NewDualAdapter(metadataAdapter, dataAdapter block.Adapter) *DualAdapter {
	return &DualAdapter{
		metadataAdapter: metadataAdapter,
		dataAdapter:     dataAdapter,
	}
}

// isMetadataOperation determines if the operation should use the metadata adapter
func (d *DualAdapter) isMetadataOperation(obj block.ObjectPointer) bool {
	return obj.IdentifierType == block.IdentifierTypeRelative && strings.HasPrefix(obj.Identifier, defaultLakeFSPrefix)
}

// getAdapter returns the appropriate adapter for the operation
func (d *DualAdapter) getAdapter(obj block.ObjectPointer) block.Adapter {
	if d.isMetadataOperation(obj) {
		return d.metadataAdapter
	}
	return d.dataAdapter
}

// Core adapter methods - delegate to appropriate adapter

func (d *DualAdapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) (*block.PutResponse, error) {
	return d.getAdapter(obj).Put(ctx, obj, sizeBytes, reader, opts)
}

func (d *DualAdapter) Get(ctx context.Context, obj block.ObjectPointer) (io.ReadCloser, error) {
	return d.getAdapter(obj).Get(ctx, obj)
}

func (d *DualAdapter) GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode, filename string) (string, time.Time, error) {
	return d.getAdapter(obj).GetPreSignedURL(ctx, obj, mode, filename)
}

func (d *DualAdapter) GetPresignUploadPartURL(ctx context.Context, obj block.ObjectPointer, uploadID string, partNumber int) (string, error) {
	return d.getAdapter(obj).GetPresignUploadPartURL(ctx, obj, uploadID, partNumber)
}

func (d *DualAdapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	return d.getAdapter(obj).Exists(ctx, obj)
}

func (d *DualAdapter) GetRange(ctx context.Context, obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	return d.getAdapter(obj).GetRange(ctx, obj, startPosition, endPosition)
}

func (d *DualAdapter) GetProperties(ctx context.Context, obj block.ObjectPointer) (block.Properties, error) {
	return d.getAdapter(obj).GetProperties(ctx, obj)
}

func (d *DualAdapter) Remove(ctx context.Context, obj block.ObjectPointer) error {
	return d.getAdapter(obj).Remove(ctx, obj)
}

func (d *DualAdapter) Copy(ctx context.Context, sourceObj, destinationObj block.ObjectPointer) error {
	sourceAdapter := d.getAdapter(sourceObj)
	destAdapter := d.getAdapter(destinationObj)

	if sourceAdapter != destAdapter {
		return ErrCrossAdapterCopy
	}

	return sourceAdapter.Copy(ctx, sourceObj, destinationObj)
}

// Multipart upload operations

func (d *DualAdapter) CreateMultiPartUpload(ctx context.Context, obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	return d.getAdapter(obj).CreateMultiPartUpload(ctx, obj, r, opts)
}

func (d *DualAdapter) UploadPart(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	return d.getAdapter(obj).UploadPart(ctx, obj, sizeBytes, reader, uploadID, partNumber)
}

func (d *DualAdapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	sourceAdapter := d.getAdapter(sourceObj)
	destAdapter := d.getAdapter(destinationObj)

	if sourceAdapter != destAdapter {
		return nil, ErrCrossAdapterCopy
	}

	return sourceAdapter.UploadCopyPart(ctx, sourceObj, destinationObj, uploadID, partNumber)
}

func (d *DualAdapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*block.UploadPartResponse, error) {
	sourceAdapter := d.getAdapter(sourceObj)
	destAdapter := d.getAdapter(destinationObj)

	if sourceAdapter != destAdapter {
		return nil, ErrCrossAdapterCopy
	}

	return sourceAdapter.UploadCopyPartRange(ctx, sourceObj, destinationObj, uploadID, partNumber, startPosition, endPosition)
}

func (d *DualAdapter) ListParts(ctx context.Context, obj block.ObjectPointer, uploadID string, opts block.ListPartsOpts) (*block.ListPartsResponse, error) {
	return d.getAdapter(obj).ListParts(ctx, obj, uploadID, opts)
}

func (d *DualAdapter) ListMultipartUploads(ctx context.Context, obj block.ObjectPointer, opts block.ListMultipartUploadsOpts) (*block.ListMultipartUploadsResponse, error) {
	return d.getAdapter(obj).ListMultipartUploads(ctx, obj, opts)
}

func (d *DualAdapter) AbortMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string) error {
	return d.getAdapter(obj).AbortMultiPartUpload(ctx, obj, uploadID)
}

func (d *DualAdapter) CompleteMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	return d.getAdapter(obj).CompleteMultiPartUpload(ctx, obj, uploadID, multipartList)
}

// Walker and utility methods

func (d *DualAdapter) GetWalker(storageID string, opts block.WalkerOptions) (block.Walker, error) {
	return d.dataAdapter.GetWalker(storageID, opts)
}

func (d *DualAdapter) BlockstoreType() string {
	return block.BlockstoreTypeGS
}

func (d *DualAdapter) BlockstoreMetadata(ctx context.Context) (*block.BlockstoreMetadata, error) {
	return nil, block.ErrOperationNotSupported
}

func (d *DualAdapter) GetStorageNamespaceInfo(storageID string) *block.StorageNamespaceInfo {
	return d.dataAdapter.GetStorageNamespaceInfo(storageID)
}

func (d *DualAdapter) ResolveNamespace(storageID, storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	return d.dataAdapter.ResolveNamespace(storageID, storageNamespace, key, identifierType)
}

func (d *DualAdapter) GetRegion(ctx context.Context, storageID, storageNamespace string) (string, error) {
	return "", block.ErrOperationNotSupported
}

func (d *DualAdapter) RuntimeStats() map[string]string {
	return nil
}
