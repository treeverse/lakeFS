package gs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
)

const (
	BlockstoreType = "gs"
)

var (
	ErrNotImplemented = fmt.Errorf("not implemented")
)

func resolveNamespace(obj block.ObjectPointer) (block.QualifiedKey, error) {
	qualifiedKey, err := block.ResolveNamespace(obj.StorageNamespace, obj.Identifier)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.StorageType != block.StorageTypeGS {
		return qualifiedKey, block.ErrInvalidNamespace
	}
	return qualifiedKey, nil
}

type Adapter struct {
	client                *storage.Client
	ctx                   context.Context
	uploadIDTranslator    block.UploadIDTranslator
	streamingChunkSize    int
	streamingChunkTimeout time.Duration
}

func WithStreamingChunkSize(sz int) func(a *Adapter) {
	return func(a *Adapter) {
		a.streamingChunkSize = sz
	}
}

func WithStreamingChunkTimeout(d time.Duration) func(a *Adapter) {
	return func(a *Adapter) {
		a.streamingChunkTimeout = d
	}
}

func WithContext(ctx context.Context) func(a *Adapter) {
	return func(a *Adapter) {
		a.ctx = ctx
	}
}

func WithTranslator(t block.UploadIDTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIDTranslator = t
	}
}

func NewAdapter(client *storage.Client, opts ...func(a *Adapter)) block.Adapter {
	a := &Adapter{
		client:             client,
		ctx:                context.Background(),
		uploadIDTranslator: &block.NoOpTranslator{},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (s *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		ctx:                   ctx,
		client:                s.client,
		uploadIDTranslator:    s.uploadIDTranslator,
		streamingChunkSize:    s.streamingChunkSize,
		streamingChunkTimeout: s.streamingChunkTimeout,
	}
}

func (s *Adapter) Put(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	w := s.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		NewWriter(s.ctx)
	_, err = io.Copy(w, reader)
	if err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}
	err = w.Close()
	if err != nil {
		return fmt.Errorf("Writer.Close: %w", err)
	}
	return nil
}

func (s *Adapter) UploadPart(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), &sizeBytes, &err)
	err = fmt.Errorf("upload part %w", ErrNotImplemented)
	return "", err
}

func (s *Adapter) Get(obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("Get", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	r, err := s.client.Bucket(qualifiedKey.StorageNamespace).Object(qualifiedKey.Key).NewReader(s.ctx)
	return r, err
}

func (s *Adapter) GetRange(obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("GetRange", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	r, err := s.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		NewRangeReader(s.ctx, startPosition, endPosition-startPosition)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *Adapter) GetProperties(obj block.ObjectPointer) (block.Properties, error) {
	var err error
	defer reportMetrics("GetProperties", time.Now(), nil, &err)
	var props block.Properties
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return props, err
	}
	_, err = s.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		Attrs(s.ctx)
	if err != nil {
		return props, err
	}
	return props, nil
}

func (s *Adapter) Remove(obj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Remove", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	err = s.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		Delete(s.ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).Delete: %w", qualifiedKey.Key, err)
	}
	return nil
}

func (s *Adapter) CreateMultiPartUpload(obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (string, error) {
	var err error
	defer reportMetrics("CreateMultiPartUpload", time.Now(), nil, &err)
	err = fmt.Errorf("multi part upload %w", ErrNotImplemented)
	return "", err
}

func (s *Adapter) AbortMultiPartUpload(obj block.ObjectPointer, uploadID string) error {
	var err error
	defer reportMetrics("AbortMultiPartUpload", time.Now(), nil, &err)
	err = fmt.Errorf("multi part upload %w", ErrNotImplemented)
	return err
}

func (s *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	err = fmt.Errorf("multi part upload %w", ErrNotImplemented)
	return nil, 0, err
}

func (s *Adapter) ValidateConfiguration(_ string) error {
	return nil
}

func (s *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string) (block.Inventory, error) {
	return nil, fmt.Errorf("inventory %w", ErrNotImplemented)
}
