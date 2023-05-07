package mem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	ErrNoDataForKey            = fmt.Errorf("no data for key: %w", block.ErrDataNotFound)
	ErrMultiPartNotFound       = fmt.Errorf("multipart ID not found")
	ErrNoPropertiesForKey      = fmt.Errorf("no properties for key")
	ErrInventoryNotImplemented = errors.New("inventory feature not implemented for memory storage adapter")
)

type mpu struct {
	id    string
	parts map[int][]byte
}

func newMPU() *mpu {
	uid := uuid.New()
	uploadID := hex.EncodeToString(uid[:])
	return &mpu{
		id:    uploadID,
		parts: make(map[int][]byte),
	}
}

func (m *mpu) get() []byte {
	buf := bytes.NewBuffer(nil)
	keys := make([]int, len(m.parts))
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	for _, part := range keys {
		buf.Write(m.parts[part])
	}
	return buf.Bytes()
}

type Adapter struct {
	data       map[string][]byte
	mpu        map[string]*mpu
	properties map[string]block.Properties
	mutex      *sync.RWMutex
}

func New(opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		data:       make(map[string][]byte),
		mpu:        make(map[string]*mpu),
		properties: make(map[string]block.Properties),
		mutex:      &sync.RWMutex{},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func getKey(obj block.ObjectPointer) string {
	// TODO (niro): Fix mem storage path resolution
	if obj.IdentifierType == block.IdentifierTypeFull {
		return obj.Identifier
	}
	return fmt.Sprintf("%s:%s", obj.StorageNamespace, obj.Identifier)
}

func (a *Adapter) Put(_ context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, opts block.PutOpts) error {
	if err := verifyObjectPointer(obj); err != nil {
		return err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	key := getKey(obj)
	a.data[key] = data
	a.properties[key] = block.Properties(opts)
	return nil
}

func (a *Adapter) Get(_ context.Context, obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	key := getKey(obj)
	data, ok := a.data[key]
	if !ok {
		return nil, ErrNoDataForKey
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func verifyObjectPointer(obj block.ObjectPointer) error {
	if !strings.HasPrefix(obj.StorageNamespace, "mem://") {
		return fmt.Errorf("mem block adapter: %w storage namespace: %s", block.ErrInvalidAddress, obj.StorageNamespace)
	}
	return nil
}

func (a *Adapter) GetWalker(_ *url.URL) (block.Walker, error) {
	return nil, fmt.Errorf("mem block adapter: %w", block.ErrOperationNotSupported)
}

func (a *Adapter) GetPreSignedURL(_ context.Context, obj block.ObjectPointer, _ block.PreSignMode) (string, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return "", err
	}
	return "", fmt.Errorf("mem block adapter: %w", block.ErrOperationNotSupported)
}

func (a *Adapter) Exists(_ context.Context, obj block.ObjectPointer) (bool, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return false, err
	}
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	_, ok := a.data[getKey(obj)]
	return ok, nil
}

func (a *Adapter) GetRange(_ context.Context, obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	data, ok := a.data[getKey(obj)]
	if !ok {
		return nil, ErrNoDataForKey
	}
	return io.NopCloser(io.NewSectionReader(bytes.NewReader(data), startPosition, endPosition-startPosition+1)), nil
}

func (a *Adapter) GetProperties(_ context.Context, obj block.ObjectPointer) (block.Properties, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return block.Properties{}, err
	}
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	props, ok := a.properties[getKey(obj)]
	if !ok {
		return block.Properties{}, ErrNoPropertiesForKey
	}
	return props, nil
}

func (a *Adapter) Remove(_ context.Context, obj block.ObjectPointer) error {
	if err := verifyObjectPointer(obj); err != nil {
		return err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.data, getKey(obj))
	return nil
}

func (a *Adapter) Copy(_ context.Context, sourceObj, destinationObj block.ObjectPointer) error {
	if err := verifyObjectPointer(sourceObj); err != nil {
		return err
	}
	if err := verifyObjectPointer(destinationObj); err != nil {
		return err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	destinationKey := getKey(destinationObj)
	sourceKey := getKey(sourceObj)
	a.data[destinationKey] = a.data[sourceKey]
	a.properties[destinationKey] = a.properties[sourceKey]
	return nil
}

func (a *Adapter) UploadCopyPart(ctx context.Context, sourceObj, _ block.ObjectPointer, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	if err := verifyObjectPointer(sourceObj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	mpu, ok := a.mpu[uploadID]
	if !ok {
		return nil, ErrMultiPartNotFound
	}
	entry, err := a.Get(ctx, sourceObj, 0)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(entry)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	_, err = h.Write(data)
	if err != nil {
		return nil, err
	}
	code := h.Sum(nil)
	mpu.parts[partNumber] = data
	etag := fmt.Sprintf("%x", code)
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (a *Adapter) UploadCopyPartRange(_ context.Context, sourceObj, _ block.ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*block.UploadPartResponse, error) {
	if err := verifyObjectPointer(sourceObj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	mpu, ok := a.mpu[uploadID]
	if !ok {
		return nil, ErrMultiPartNotFound
	}
	data, ok := a.data[getKey(sourceObj)]
	if !ok {
		return nil, ErrNoDataForKey
	}
	reader := io.NewSectionReader(bytes.NewReader(data), startPosition, endPosition-startPosition+1)
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
	mpu.parts[partNumber] = data
	etag := fmt.Sprintf("%x", code)
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (a *Adapter) CreateMultiPartUpload(_ context.Context, obj block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	mpu := newMPU()
	a.mpu[mpu.id] = mpu
	return &block.CreateMultiPartUploadResponse{
		UploadID: mpu.id,
	}, nil
}

func (a *Adapter) UploadPart(_ context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	mpu, ok := a.mpu[uploadID]
	if !ok {
		return nil, ErrMultiPartNotFound
	}
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
	mpu.parts[partNumber] = data
	etag := fmt.Sprintf("%x", code)
	return &block.UploadPartResponse{
		ETag: etag,
	}, nil
}

func (a *Adapter) AbortMultiPartUpload(_ context.Context, obj block.ObjectPointer, uploadID string) error {
	if err := verifyObjectPointer(obj); err != nil {
		return err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	_, ok := a.mpu[uploadID]
	if !ok {
		return ErrMultiPartNotFound
	}
	delete(a.mpu, uploadID)
	return nil
}

func (a *Adapter) CompleteMultiPartUpload(_ context.Context, obj block.ObjectPointer, uploadID string, _ *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	if err := verifyObjectPointer(obj); err != nil {
		return nil, err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	mpu, ok := a.mpu[uploadID]
	if !ok {
		return nil, ErrMultiPartNotFound
	}
	data := mpu.get()
	h := sha256.New()
	_, err := h.Write(data)
	if err != nil {
		return nil, err
	}
	code := h.Sum(nil)
	hexCode := fmt.Sprintf("%x", code)
	a.data[getKey(obj)] = data
	return &block.CompleteMultiPartUploadResponse{
		ETag:          hexCode,
		ContentLength: int64(len(data)),
	}, nil
}

func (a *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, ErrInventoryNotImplemented
}

func (a *Adapter) BlockstoreType() string {
	return block.BlockstoreTypeMem
}

func (a *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	info := block.DefaultStorageNamespaceInfo(block.BlockstoreTypeMem)
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
