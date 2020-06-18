package mem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
)

type mpu struct {
	id    string
	parts map[int64][]byte
}

func newMPU() *mpu {
	UUIDBytes := [16]byte(uuid.New())
	uploadId := hex.EncodeToString(UUIDBytes[:])
	return &mpu{
		id:    uploadId,
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
	uploadIdTranslator block.UploadIdTranslator
}

func New(opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		ctx:                context.Background(),
		uploadIdTranslator: &block.NoOpTranslator{},
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

func WithTranslator(t block.UploadIdTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIdTranslator = t
	}
}

func getKey(obj block.ObjectPointer) string {
	return fmt.Sprintf("%s:%s", obj.Repo, obj.Identifier)
}

func (a *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		ctx:                ctx,
		data:               a.data,
		mpu:                a.mpu,
		properties:         a.properties,
		mutex:              a.mutex,
		uploadIdTranslator: a.uploadIdTranslator,
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

func (a *Adapter) Get(obj block.ObjectPointer) (io.ReadCloser, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	data, ok := a.data[getKey(obj)]
	if !ok {
		return nil, fmt.Errorf("no data for key")
	}
	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (a *Adapter) GetRange(obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	data, ok := a.data[getKey(obj)]
	if !ok {
		return nil, fmt.Errorf("no data for key")
	}
	return ioutil.NopCloser(io.NewSectionReader(bytes.NewReader(data), startPosition, endPosition-startPosition+1)), nil
}

func (a *Adapter) GetProperties(obj block.ObjectPointer) (block.Properties, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	props, ok := a.properties[getKey(obj)]
	if !ok {
		return block.Properties{}, fmt.Errorf("no properties for key")
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
	tid := a.uploadIdTranslator.SetUploadId(mpu.id)
	return tid, nil
}

func (a *Adapter) UploadPart(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (string, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	uploadId = a.uploadIdTranslator.TranslateUploadId(uploadId)
	mpu, ok := a.mpu[uploadId]
	if !ok {
		return "", fmt.Errorf("multipart ID not found")
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write(data)
	code := h.Sum(nil)
	mpu.parts[partNumber] = data
	return fmt.Sprintf("%x", code), nil
}

func (a *Adapter) AbortMultiPartUpload(obj block.ObjectPointer, uploadId string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	uploadId = a.uploadIdTranslator.TranslateUploadId(uploadId)
	_, ok := a.mpu[uploadId]
	if !ok {
		return fmt.Errorf("multipart ID not found")
	}
	delete(a.mpu, uploadId)
	a.uploadIdTranslator.RemoveUploadId(uploadId)
	return nil
}

func (a *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadId string, MultipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	uploadId = a.uploadIdTranslator.TranslateUploadId(uploadId)
	mpu, ok := a.mpu[uploadId]
	if !ok {
		return nil, 0, fmt.Errorf("multipart ID not found")
	}
	data := mpu.get()
	h := sha256.New()
	h.Write(data)
	code := h.Sum(nil)
	hex := fmt.Sprintf("%x", code)
	a.uploadIdTranslator.RemoveUploadId(uploadId)
	a.data[getKey(obj)] = data
	return &hex, int64(len(data)), nil
}
