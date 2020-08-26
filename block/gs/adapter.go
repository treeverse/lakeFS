package gs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"cloud.google.com/go/storage"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/logging"
	"google.golang.org/api/iterator"
)

const (
	BlockstoreType      = "gs"
	MaxMultipartObjects = 10000
)

var (
	ErrNotImplemented      = fmt.Errorf("not implemented")
	ErrMissingPartNumber   = errors.New("missing part number")
	ErrMissingPartETag     = errors.New("missing part ETag")
	ErrMismatchPartETag    = errors.New("mismatch part ETag")
	ErrMismatchPartName    = errors.New("mismatch part name")
	ErrMaxMultipartObjects = errors.New("maximum multipart object reached")
	ErrPartListMismatch    = errors.New("multipart part list mismatch")
	ErrMissingTargetAttrs  = errors.New("missing target attributes")
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
		ctx:                context.Background(),
		client:             client,
		uploadIDTranslator: &block.NoOpTranslator{},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		ctx:                ctx,
		client:             a.client,
		uploadIDTranslator: a.uploadIDTranslator,
	}
}

func (a *Adapter) log() logging.Logger {
	return logging.FromContext(a.ctx)
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

func (a *Adapter) Put(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, _ block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	w := a.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		NewWriter(a.ctx)
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

func (a *Adapter) Get(obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("Get", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	r, err := a.client.Bucket(qualifiedKey.StorageNamespace).Object(qualifiedKey.Key).NewReader(a.ctx)
	return r, err
}

func (a *Adapter) GetRange(obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("GetRange", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	r, err := a.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		NewRangeReader(a.ctx, startPosition, endPosition-startPosition+1)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (a *Adapter) GetProperties(obj block.ObjectPointer) (block.Properties, error) {
	var err error
	defer reportMetrics("GetProperties", time.Now(), nil, &err)
	var props block.Properties
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return props, err
	}
	_, err = a.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		Attrs(a.ctx)
	if err != nil {
		return props, err
	}
	return props, nil
}

func (a *Adapter) Remove(obj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Remove", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	err = a.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		Delete(a.ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).Delete: %w", qualifiedKey.Key, err)
	}
	return nil
}

func (a *Adapter) CreateMultiPartUpload(obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (string, error) {
	var err error
	defer reportMetrics("CreateMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return "", err
	}
	// we use the qualified key as the upload id
	uploadID := a.uploadIDTranslator.SetUploadID(qualifiedKey.Key)
	// we keep a marker file to identify multipart in progress
	objName := formatMultipartMarkerFilename(uploadID)
	o := a.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(objName)
	w := o.NewWriter(a.ctx)
	_, err = io.WriteString(w, qualifiedKey.Key)
	if err != nil {
		return "", fmt.Errorf("io.WriteString: %w", err)
	}
	err = w.Close()
	if err != nil {
		return "", fmt.Errorf("writer.Close: %w", err)
	}
	// log information
	a.log().WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.StorageNamespace,
		"qualified_key": qualifiedKey.Key,
		"key":           obj.Identifier,
	}).Debug("created multipart upload")
	return uploadID, nil
}

func (a *Adapter) UploadPart(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return "", err
	}
	objName := formatMultipartFilename(uploadID, partNumber)
	o := a.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(objName)
	w := o.NewWriter(a.ctx)
	_, err = io.Copy(w, reader)
	if err != nil {
		return "", fmt.Errorf("io.Copy: %w", err)
	}
	err = w.Close()
	if err != nil {
		return "", fmt.Errorf("writer.Close: %w", err)
	}
	attrs, err := o.Attrs(a.ctx)
	if err != nil {
		return "", fmt.Errorf("object.Attrs: %w", err)
	}
	return attrs.Etag, nil
}

func (a *Adapter) AbortMultiPartUpload(obj block.ObjectPointer, uploadID string) error {
	var err error
	defer reportMetrics("AbortMultiPartUpload", time.Now(), nil, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	bucket := a.client.Bucket(qualifiedKey.StorageNamespace)

	// delete all related files by listing the prefix
	it := bucket.Objects(a.ctx, &storage.Query{
		Prefix:    uploadID,
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
		if err := bucket.Object(attrs.Name).Delete(a.ctx); err != nil {
			return fmt.Errorf("bucket(%s).object(%s).Delete(): %w", qualifiedKey.StorageNamespace, attrs.Name, err)
		}
	}
	return nil
}

func (a *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, 0, err
	}
	lg := a.log().WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.StorageNamespace,
		"qualified_key": qualifiedKey.Key,
		"key":           obj.Identifier,
	})

	// list bucket parts and validate request match
	bucket := a.client.Bucket(qualifiedKey.StorageNamespace)
	bucketParts, err := a.listMultipartUploadParts(bucket, uploadID, qualifiedKey)
	if err != nil {
		return nil, 0, err
	}
	// validate bucketParts match the request multipartList
	err = a.validateMultipartUploadParts(qualifiedKey, multipartList, bucketParts)
	if err != nil {
		return nil, 0, err
	}

	// prepare names
	parts := make([]string, len(bucketParts))
	for i, part := range bucketParts {
		parts[i] = part.Name
	}

	// compose target object
	targetAttrs, err := a.composeMultipartUploadParts(bucket, qualifiedKey, parts)
	if err != nil {
		lg.WithError(err).Error("CompleteMultipartUpload failed")
		return nil, 0, err
	}

	// delete marker
	objMarker := bucket.Object(formatMultipartMarkerFilename(uploadID))
	if err := objMarker.Delete(a.ctx); err != nil {
		a.log().WithError(err).Warn("Failed to delete multipart upload marker")
	}
	lg.Debug("completed multipart upload")
	return &targetAttrs.Etag, targetAttrs.Size, nil
}

func (a *Adapter) validateMultipartUploadParts(qualifiedKey block.QualifiedKey, multipartList *block.MultipartUploadCompletion, bucketParts []*storage.ObjectAttrs) error {
	if len(multipartList.Part) != len(bucketParts) {
		return ErrPartListMismatch
	}
	for i, p := range multipartList.Part {
		if p.PartNumber == nil {
			return fmt.Errorf("invalid part at position %d: %w", i, ErrMissingPartNumber)
		}
		if p.ETag == nil {
			return fmt.Errorf("invalid part at position %d: %w", i, ErrMissingPartETag)
		}
		objName := formatMultipartFilename(qualifiedKey.Key, *p.PartNumber)
		if objName != bucketParts[i].Name {
			return fmt.Errorf("invalid part at position %d: %w", i, ErrMismatchPartName)
		}
		if *p.ETag != bucketParts[i].Etag {
			return fmt.Errorf("invalid part at position %d: %w", i, ErrMismatchPartETag)
		}
	}
	return nil
}

func (a *Adapter) listMultipartUploadParts(bucket *storage.BucketHandle, uploadID string, qualifiedKey block.QualifiedKey) ([]*storage.ObjectAttrs, error) {
	var bucketParts []*storage.ObjectAttrs
	it := bucket.Objects(a.ctx, &storage.Query{
		Delimiter: catalog.DefaultPathDelimiter,
		Prefix:    uploadID + ".part_",
	})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("bucket(%s).Objects(): %w", qualifiedKey.StorageNamespace, err)
		}
		bucketParts = append(bucketParts, attrs)
		if len(bucketParts) > MaxMultipartObjects {
			return nil, fmt.Errorf("bucket(%s).Objects(): %w", qualifiedKey.StorageNamespace, ErrMaxMultipartObjects)
		}
	}
	// sort by name - assume natual sort order
	sort.Slice(bucketParts, func(i, j int) bool {
		return bucketParts[i].Name < bucketParts[j].Name
	})
	return bucketParts, nil
}

func (a *Adapter) composeMultipartUploadParts(bucket *storage.BucketHandle, qualifiedKey block.QualifiedKey, parts []string) (*storage.ObjectAttrs, error) {
	// compose target from all parts
	var targetAttrs *storage.ObjectAttrs
	err := ComposeAll(qualifiedKey.Key, parts, func(target string, parts []string) error {
		objs := make([]*storage.ObjectHandle, len(parts))
		for i := range parts {
			objs[i] = bucket.Object(parts[i])
		}
		// compose target from parts
		attrs, err := bucket.Object(target).ComposerFrom(objs...).Run(a.ctx)
		if err != nil {
			return err
		}
		if target == qualifiedKey.Key {
			targetAttrs = attrs
		}
		// delete parts
		for _, o := range objs {
			if err := o.Delete(a.ctx); err != nil {
				a.log().WithError(err).Warn("Failed to delete multipart upload part while compose")
			}
		}
		return nil
	})
	if err == nil && targetAttrs == nil {
		return nil, ErrMissingTargetAttrs
	}
	if err != nil {
		return nil, err
	}
	return targetAttrs, nil
}

func (a *Adapter) ValidateConfiguration(_ string) error {
	return nil
}

func (a *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string) (block.Inventory, error) {
	return nil, fmt.Errorf("inventory %w", ErrNotImplemented)
}

func (a *Adapter) Close() error {
	return a.client.Close()
}

func formatMultipartFilename(uploadID string, partNumber int64) string {
	// keep natural sort order with zero padding
	return fmt.Sprintf("%s.part_%05d", uploadID, partNumber)
}

func formatMultipartMarkerFilename(uploadID string) string {
	return uploadID + ".multipart"
}
