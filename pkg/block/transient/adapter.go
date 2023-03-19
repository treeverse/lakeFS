package transient

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

var ErrInventoryNotImplemented = errors.New("inventory feature not implemented for transient storage adapter")

type Adapter struct{}

func New() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Put(_ context.Context, _ block.ObjectPointer, _ int64, reader io.Reader, _ block.PutOpts) error {
	_, err := io.Copy(io.Discard, reader)
	return err
}

func (a *Adapter) Get(_ context.Context, _ block.ObjectPointer, expectedSize int64) (io.ReadCloser, error) {
	if expectedSize < 0 {
		return nil, io.ErrUnexpectedEOF
	}
	return io.NopCloser(&io.LimitedReader{R: rand.Reader, N: expectedSize}), nil
}

func (a *Adapter) GetWalker(_ *url.URL) (block.Walker, error) {
	return nil, block.ErrOperationNotSupported
}

func (a *Adapter) GetPreSignedURL(_ context.Context, _ block.ObjectPointer, _ block.PreSignMode) (string, error) {
	return "", block.ErrOperationNotSupported
}

func (a *Adapter) Exists(_ context.Context, _ block.ObjectPointer) (bool, error) {
	return true, nil
}

func (a *Adapter) GetRange(_ context.Context, _ block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	n := endPosition - startPosition
	if n < 0 {
		return nil, io.ErrUnexpectedEOF
	}
	reader := &io.LimitedReader{
		R: rand.Reader,
		N: n,
	}
	return io.NopCloser(reader), nil
}

func (a *Adapter) GetProperties(_ context.Context, _ block.ObjectPointer) (block.Properties, error) {
	return block.Properties{}, nil
}

func (a *Adapter) Remove(_ context.Context, _ block.ObjectPointer) error {
	return nil
}

func (a *Adapter) Copy(_ context.Context, _, _ block.ObjectPointer) error {
	return nil
}

func (a *Adapter) UploadCopyPart(_ context.Context, _, _ block.ObjectPointer, _ string, _ int) (*block.UploadPartResponse, error) {
	h := sha256.New()
	code := h.Sum(nil)
	etag := hex.EncodeToString(code)
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (a *Adapter) UploadCopyPartRange(_ context.Context, _, _ block.ObjectPointer, _ string, _ int, startPosition, endPosition int64) (*block.UploadPartResponse, error) {
	n := endPosition - startPosition
	if n < 0 {
		return nil, io.ErrUnexpectedEOF
	}
	h := sha256.New()
	code := h.Sum(nil)
	etag := hex.EncodeToString(code)
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (a *Adapter) CreateMultiPartUpload(_ context.Context, _ block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	uid := uuid.New()
	uploadID := hex.EncodeToString(uid[:])
	return &block.CreateMultiPartUploadResponse{
		UploadID: uploadID,
	}, nil
}

func (a *Adapter) UploadPart(_ context.Context, _ block.ObjectPointer, _ int64, reader io.Reader, _ string, _ int) (*block.UploadPartResponse, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	_, err = h.Write(data)
	if err != nil {
		return nil, err
	}
	code := h.Sum(nil)
	etag := hex.EncodeToString(code)
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (a *Adapter) AbortMultiPartUpload(context.Context, block.ObjectPointer, string) error {
	return nil
}

func (a *Adapter) CompleteMultiPartUpload(context.Context, block.ObjectPointer, string, *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	const dataSize = 1024
	data := make([]byte, dataSize)
	if _, err := rand.Read(data); err != nil {
		return nil, err
	}

	h := sha256.New()
	_, err := h.Write(data)
	if err != nil {
		return nil, err
	}
	code := h.Sum(nil)
	codeHex := hex.EncodeToString(code)
	return &block.CompleteMultiPartUploadResponse{
		ETag:          codeHex,
		ContentLength: dataSize,
	}, nil
}

func (a *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, ErrInventoryNotImplemented
}

func (a *Adapter) BlockstoreType() string {
	return block.BlockstoreTypeTransient
}

func (a *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	info := block.DefaultStorageNamespaceInfo(block.BlockstoreTypeTransient)
	info.PreSignSupport = false
	info.ImportSupport = false
	return info
}

func (a *Adapter) ResolveNamespace(storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	return block.DefaultResolveNamespace(storageNamespace, key, identifierType)
}

func (a *Adapter) RuntimeStats() map[string]string {
	return nil
}
