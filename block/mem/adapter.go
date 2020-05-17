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
	mutex              *sync.RWMutex
	uploadIdTranslator block.UploadIdTranslator
}

func New(opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		ctx:                context.Background(),
		uploadIdTranslator: &block.NoOpTranslator{},
		data:               make(map[string][]byte),
		mpu:                make(map[string]*mpu),
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

func getKey(repo, identifier string) string {
	return fmt.Sprintf("%s:%s", repo, identifier)
}

func (a *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		ctx:                ctx,
		data:               a.data,
		mpu:                a.mpu,
		mutex:              a.mutex,
		uploadIdTranslator: a.uploadIdTranslator,
	}
}

func (a *Adapter) Put(repo string, identifier string, sizeBytes int64, reader io.Reader) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	a.data[getKey(repo, identifier)] = data
	return nil
}

func (a *Adapter) Get(repo string, identifier string) (io.ReadCloser, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	data, ok := a.data[getKey(repo, identifier)]
	if !ok {
		return nil, fmt.Errorf("no data for key")
	}
	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (a *Adapter) GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	data, ok := a.data[getKey(repo, identifier)]
	if !ok {
		return nil, fmt.Errorf("no data for key")
	}
	return ioutil.NopCloser(io.NewSectionReader(bytes.NewReader(data), startPosition, endPosition-startPosition+1)), nil
}

func (a *Adapter) Remove(repo string, identifier string) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	delete(a.data, getKey(repo, identifier))
	return nil
}

func (a *Adapter) CreateMultiPartUpload(repo string, identifier string, r *http.Request) (string, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	mpu := newMPU()
	a.mpu[mpu.id] = mpu
	tid := a.uploadIdTranslator.SetUploadId(mpu.id)
	return tid, nil
}

func (a *Adapter) UploadPart(repo string, identifier string, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (string, error) {
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

func (a *Adapter) AbortMultiPartUpload(repo string, identifier string, uploadId string) error {
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

func (a *Adapter) CompleteMultiPartUpload(repo string, identifier string, uploadId string, MultipartList *block.MultipartUploadCompletion) (*string, int64, error) {
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
	a.data[getKey(repo, identifier)] = data
	return &hex, int64(len(data)), nil
}
