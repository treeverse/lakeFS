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

func (a *Adapter) Put(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
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
	uploadID = a.uploadIDTranslator.SetUploadID(uploadID)
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
	uploadID = a.uploadIDTranslator.TranslateUploadID(uploadID)
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
	a.uploadIDTranslator.RemoveUploadID(uploadID)
	return nil
}

func (a *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, 0, err
	}
	// uploadID translation
	uploadID = a.uploadIDTranslator.TranslateUploadID(uploadID)
	lg := a.log().WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.StorageNamespace,
		"qualified_key": qualifiedKey.Key,
		"key":           obj.Identifier,
	})

	bucket := a.client.Bucket(qualifiedKey.StorageNamespace)

	// list bucket parts and validate request match
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
			return nil, 0, fmt.Errorf("bucket(%s).Objects(): %w", qualifiedKey.StorageNamespace, err)
		}
		bucketParts = append(bucketParts, attrs)
		if len(bucketParts) > MaxMultipartObjects {
			return nil, 0, fmt.Errorf("bucket(%s).Objects(): %w", qualifiedKey.StorageNamespace, ErrMaxMultipartObjects)
		}
	}
	// sort bucketParts by name
	sort.Slice(bucketParts, func(i, j int) bool {
		return bucketParts[i].Name < bucketParts[j].Name
	})
	// validate bucketParts match the request multipartList
	if len(multipartList.Part) != len(bucketParts) {
		return nil, 0, ErrPartListMismatch
	}
	// compose one file from all parts by listing
	objParts := make([]*storage.ObjectHandle, len(multipartList.Part))
	for i, p := range multipartList.Part {
		if p.PartNumber == nil {
			return nil, 0, fmt.Errorf("invalid part at position %d: %w", i, ErrMissingPartNumber)
		}
		if p.ETag == nil {
			return nil, 0, fmt.Errorf("invalid part at position %d: %w", i, ErrMissingPartETag)
		}
		objName := fmt.Sprintf("%s.part_%d", qualifiedKey.Key, *p.PartNumber)
		if objName != bucketParts[i].Name {
			return nil, 0, fmt.Errorf("invalid part at position %d: %w", i, ErrMismatchPartName)
		}
		objParts[i] = bucket.Object(objName)
		objAttrs, err := objParts[i].Attrs(a.ctx)
		if err != nil {
			return nil, 0, err
		}
		if objAttrs.Etag != *p.ETag {
			return nil, 0, fmt.Errorf("invalid part at position %d: %w", i, ErrMismatchPartETag)
		}
	}

	attrs, err := a.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		ComposerFrom(objParts...).
		Run(a.ctx)
	if err != nil {
		lg.WithError(err).Error("CompleteMultipartUpload failed")
		return nil, 0, err
	}

	// delete parts and marker file
	nameMarker := formatMultipartMarkerFilename(uploadID)
	objMarker := bucket.Object(nameMarker)
	objParts = append(objParts, objMarker)
	for _, src := range objParts {
		if err := src.Delete(a.ctx); err != nil {
			a.log().WithError(err).Warn("Failed to delete multipart upload while compose")
		}
	}
	lg.Debug("completed multipart upload")
	a.uploadIDTranslator.RemoveUploadID(uploadID)
	return &attrs.Etag, attrs.Size, nil
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
	return fmt.Sprintf("%s.part_%d", uploadID, partNumber)
}

func formatMultipartMarkerFilename(uploadID string) string {
	return uploadID + ".multipart"
}
