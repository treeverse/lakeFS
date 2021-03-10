package transient

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

const BlockstoreType = "transient"

var ErrInventoryNotImplemented = errors.New("inventory feature not implemented for transient storage adapter")

type Adapter struct{}

func New() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Put(_ context.Context, _ block.ObjectPointer, _ int64, reader io.Reader, _ block.PutOpts) error {
	_, err := io.Copy(ioutil.Discard, reader)
	return err
}

func (a *Adapter) Get(_ context.Context, obj block.ObjectPointer, expectedSize int64) (io.ReadCloser, error) {
	if expectedSize < 0 {
		return nil, io.ErrUnexpectedEOF
	}
	return ioutil.NopCloser(&io.LimitedReader{R: rand.Reader, N: expectedSize}), nil
}

func (a *Adapter) Exists(_ context.Context, obj block.ObjectPointer) (bool, error) {
	return true, nil
}

func (a *Adapter) GetRange(_ context.Context, obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	n := endPosition - startPosition
	if n < 0 {
		return nil, io.ErrUnexpectedEOF
	}
	reader := &io.LimitedReader{
		R: rand.Reader,
		N: n,
	}
	return ioutil.NopCloser(reader), nil
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

func (a *Adapter) UploadCopyPart(_ context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int64) (string, error) {
	h := sha256.New()
	code := h.Sum(nil)
	return hex.EncodeToString(code), nil
}

func (a *Adapter) UploadCopyPartRange(_ context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber, startPosition, endPosition int64) (string, error) {
	n := endPosition - startPosition
	if n < 0 {
		return "", io.ErrUnexpectedEOF
	}
	h := sha256.New()
	code := h.Sum(nil)
	return hex.EncodeToString(code), nil
}

func (a *Adapter) Walk(_ context.Context, walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	return nil
}

func (a *Adapter) CreateMultiPartUpload(_ context.Context, obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (string, error) {
	uid := uuid.New()
	uploadID := hex.EncodeToString(uid[:])
	return uploadID, nil
}

func (a *Adapter) UploadPart(_ context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, err = h.Write(data)
	if err != nil {
		return "", err
	}
	code := h.Sum(nil)
	return hex.EncodeToString(code), nil
}

func (a *Adapter) AbortMultiPartUpload(context.Context, block.ObjectPointer, string) error {
	return nil
}

func (a *Adapter) CompleteMultiPartUpload(context.Context, block.ObjectPointer, string, *block.MultipartUploadCompletion) (*string, int64, error) {
	const dataSize = 1024
	data := make([]byte, dataSize)
	if _, err := rand.Read(data); err != nil {
		return nil, 0, err
	}

	h := sha256.New()
	_, err := h.Write(data)
	if err != nil {
		return nil, 0, err
	}
	code := h.Sum(nil)
	codeHex := hex.EncodeToString(code)
	return &codeHex, dataSize, nil
}

func (a *Adapter) ValidateConfiguration(_ context.Context, _ string) error {
	return nil
}

func (a *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, ErrInventoryNotImplemented
}

func (a *Adapter) BlockstoreType() string {
	return BlockstoreType
}
func (a *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	return block.DefaultStorageNamespaceInfo(BlockstoreType)
}
