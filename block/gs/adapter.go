package gs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/logging"
	"google.golang.org/api/iterator"
)

const (
	BlockstoreType = "gs"
)

var (
	ErrNotImplemented = fmt.Errorf("not implemented")
)

type Adapter struct {
	client             *storage.Client
	ctx                context.Context
	uploadIDTranslator block.UploadIDTranslator
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

func NewAdapter(client *storage.Client, opts ...func(a *Adapter)) *Adapter {
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
		ctx:                ctx,
		client:             s.client,
		uploadIDTranslator: s.uploadIDTranslator,
	}
}

func (s *Adapter) log() logging.Logger {
	return logging.FromContext(s.ctx)
}

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
		return fmt.Errorf("writer.Close: %w", err)
	}
	return nil
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
		NewRangeReader(s.ctx, startPosition, endPosition-startPosition+1)
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

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return "", err
	}
	uploadID := uuid.New().String()
	uploadID = s.uploadIDTranslator.SetUploadID(uploadID)
	s.log().WithFields(logging.Fields{
		"upload_id":            uploadID,
		"translated_upload_id": uploadID,
		"qualified_ns":         qualifiedKey.StorageNamespace,
		"qualified_key":        qualifiedKey.Key,
		"key":                  obj.Identifier,
	}).Debug("created multipart upload")
	return uploadID, nil
}

func (s *Adapter) UploadPart(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return "", err
	}
	objName := fmt.Sprintf("%s.%d", qualifiedKey.Key, partNumber)
	o := s.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(objName)
	w := o.NewWriter(s.ctx)
	_, err = io.Copy(w, reader)
	if err != nil {
		return "", fmt.Errorf("io.Copy: %w", err)
	}
	err = w.Close()
	if err != nil {
		return "", fmt.Errorf("writer.Close: %w", err)
	}
	attrs, err := o.Attrs(s.ctx)
	if err != nil {
		return "", fmt.Errorf("object.Attrs: %w", err)
	}
	return attrs.Etag, err
}

func (s *Adapter) AbortMultiPartUpload(obj block.ObjectPointer, uploadID string) error {
	var err error
	defer reportMetrics("AbortMultiPartUpload", time.Now(), nil, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	uploadID = s.uploadIDTranslator.TranslateUploadID(uploadID)
	bucket := s.client.Bucket(qualifiedKey.StorageNamespace)
	it := bucket.Objects(s.ctx, &storage.Query{
		Prefix:    qualifiedKey.Key,
		Delimiter: catalog.DefaultPathDelimiter,
	})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("bucket(%s).Objects(): %w", qualifiedKey.StorageNamespace, err)
		}
		if err := bucket.Object(attrs.Name).Delete(s.ctx); err != nil {
			return fmt.Errorf("bucket(%s).object(%s).Delete(): %w", qualifiedKey.StorageNamespace, attrs.Name, err)
		}
	}
	s.uploadIDTranslator.RemoveUploadID(uploadID)
	return nil
}

func (s *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, 0, err
	}
	// uploadID translation
	translatedUploadID := s.uploadIDTranslator.TranslateUploadID(uploadID)
	lg := s.log().WithFields(logging.Fields{
		"upload_id":            uploadID,
		"translated_upload_id": translatedUploadID,
		"qualified_ns":         qualifiedKey.StorageNamespace,
		"qualified_key":        qualifiedKey.Key,
		"key":                  obj.Identifier,
	})

	// compose one file from all parts
	bucket := s.client.Bucket(qualifiedKey.StorageNamespace)
	srcs := make([]*storage.ObjectHandle, len(multipartList.Part))
	for i := range multipartList.Part {
		var partNumber int64
		if multipartList.Part[i].PartNumber != nil {
			partNumber = *multipartList.Part[i].PartNumber
		}
		objName := fmt.Sprintf("%s.%d", qualifiedKey.Key, partNumber)
		srcs[i] = bucket.Object(objName)
	}
	composer := s.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		ComposerFrom(srcs...)
	attrs, err := composer.Run(s.ctx)
	if err != nil {
		lg.WithError(err).Error("CompleteMultipartUpload failed")
		return nil, 0, err
	}
	// delete parts
	for _, src := range srcs {
		if err := src.Delete(s.ctx); err != nil {
			s.log().WithError(err).Warn("Failed to delete multipart upload while compose")
		}
	}
	lg.Debug("completed multipart upload")
	s.uploadIDTranslator.RemoveUploadID(translatedUploadID)
	return &attrs.Etag, attrs.Size, nil
}

func (s *Adapter) ValidateConfiguration(_ string) error {
	return nil
}

func (s *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string) (block.Inventory, error) {
	return nil, fmt.Errorf("inventory %w", ErrNotImplemented)
}
