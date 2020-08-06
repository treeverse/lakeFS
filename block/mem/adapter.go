package mem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
)

const BlockstoreType = "mem"

var (
	ErrNoDataForKey            = fmt.Errorf("no data for key")
	ErrMultiPartNotFound       = fmt.Errorf("multipart ID not found")
	ErrNoPropertiesForKey      = fmt.Errorf("no properties for key")
	ErrInventoryNotImplemented = errors.New("inventory feature not implemented for memory storage adapter")
)

type mpu struct {
	id    string
	parts map[int64][]byte
}

func newMPU() *mpu {
	uid := uuid.New()
	uploadID := hex.EncodeToString(uid[:])
	return &mpu{
		id:    uploadID,
		parts: make(map[int64][]byte),
	}
}

func (m *mpu) get() []byte {
	buf := bytes.NewBuffer(nil)
	keys := make([]int64, len(m.parts))
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	for _, part := range keys {
		buf.Write(m.parts[part])
	}
	return buf.Bytes()
}

type Adapter struct {
	ctx                context.Context
	data               map[string][]byte
	mpu                map[string]*mpu
	properties         map[string]block.Properties
	mutex              *sync.RWMutex
	uploadIDTranslator block.UploadIDTranslator
}

func New(opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		ctx:                context.Background(),
		uploadIDTranslator: &block.NoOpTranslator{},
		data:               make(map[string][]byte),
		mpu:                make(map[string]*mpu),
		properties:         make(map[string]block.Properties),
		mutex:              &sync.RWMutex{},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func WithTranslator(t block.UploadIDTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIDTranslator = t
	}
}

func getKey(obj block.ObjectPointer) string {
	return fmt.Sprintf("%s:%s", obj.StorageNamespace, obj.Identifier)
}

func (a *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		ctx:                ctx,
		data:               a.data,
		mpu:                a.mpu,
		properties:         a.properties,
		mutex:              a.mutex,
		uploadIDTranslator: a.uploadIDTranslator,
	}
}

func (a *Adapter) Put(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	key := getKey(obj)
	a.data[key] = data
	a.properties[key] = block.Properties(opts)
	return nil
}

func (a *Adapter) Get(obj block.ObjectPointer, expectedSize int64) (io.ReadCloser, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	data, ok := a.data[getKey(obj)]
	if !ok {
		return nil, ErrNoDataForKey
	}
	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (a *Adapter) GetRange(obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	data, ok := a.data[getKey(obj)]
	if !ok {
		return nil, ErrNoDataForKey
	}
	return ioutil.NopCloser(io.NewSectionReader(bytes.NewReader(data), startPosition, endPosition-startPosition+1)), nil
}

func (a *Adapter) GetProperties(obj block.ObjectPointer) (block.Properties, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	props, ok := a.properties[getKey(obj)]
	if !ok {
		return block.Properties{}, ErrNoPropertiesForKey
	}
	return props, nil
}

func (a *Adapter) Remove(obj block.ObjectPointer) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.data, getKey(obj))
	return nil
}

func (a *Adapter) CreateMultiPartUpload(obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (string, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	mpu := newMPU()
	a.mpu[mpu.id] = mpu
	tid := a.uploadIDTranslator.SetUploadID(mpu.id)
	return tid, nil
}

func (a *Adapter) UploadPart(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	uploadID = a.uploadIDTranslator.TranslateUploadID(uploadID)
	mpu, ok := a.mpu[uploadID]
	if !ok {
		return "", ErrMultiPartNotFound
	}
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
	mpu.parts[partNumber] = data
	return fmt.Sprintf("%x", code), nil
}

func (a *Adapter) AbortMultiPartUpload(obj block.ObjectPointer, uploadID string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	uploadID = a.uploadIDTranslator.TranslateUploadID(uploadID)
	_, ok := a.mpu[uploadID]
	if !ok {
		return ErrMultiPartNotFound
	}
	delete(a.mpu, uploadID)
	a.uploadIDTranslator.RemoveUploadID(uploadID)
	return nil
}

func (a *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadID string, _ *block.MultipartUploadCompletion) (*string, int64, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	uploadID = a.uploadIDTranslator.TranslateUploadID(uploadID)
	mpu, ok := a.mpu[uploadID]
	if !ok {
		return nil, 0, ErrMultiPartNotFound
	}
	data := mpu.get()
	h := sha256.New()
	_, err := h.Write(data)
	if err != nil {
		return nil, 0, err
	}
	code := h.Sum(nil)
	hexCode := fmt.Sprintf("%x", code)
	a.uploadIDTranslator.RemoveUploadID(uploadID)
	a.data[getKey(obj)] = data
	return &hexCode, int64(len(data)), nil
}

func (a *Adapter) ValidateConfiguration(_ string) error {
	return nil
}

func (a *Adapter) GenerateInventory(_ logging.Logger, _ string) (block.Inventory, error) {
	return nil, ErrInventoryNotImplemented
}
