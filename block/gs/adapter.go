package gs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/logging"
	"google.golang.org/api/iterator"
)

const (
	BlockstoreType    = "gs"
	MaxPartsInCompose = 32
)

var ErrNotImplemented = fmt.Errorf("not implemented")

type Adapter struct {
	client             *storage.Client
	ctx                context.Context
	uploadIDTranslator block.UploadIDTranslator
	multipartTracker   block.MultipartTracker
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

func NewAdapter(client *storage.Client, multipartTracker block.MultipartTracker, opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		ctx:                context.Background(),
		client:             client,
		multipartTracker:   multipartTracker,
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
		multipartTracker:   a.multipartTracker,
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
	uploadID := strings.ReplaceAll(uuid.New().String(), "-", "")
	translatedUploadID := a.uploadIDTranslator.SetUploadID(uploadID)
	err = a.multipartTracker.CreateMultipartUpload(a.ctx, qualifiedKey.StorageNamespace, translatedUploadID)
	if err != nil {
		a.uploadIDTranslator.RemoveUploadID(uploadID)
		return "", err
	}
	// TODO(barak): write new upload-id entry
	a.log().WithFields(logging.Fields{
		"upload_id":            uploadID,
		"translated_upload_id": translatedUploadID,
		"qualified_ns":         qualifiedKey.StorageNamespace,
		"qualified_key":        qualifiedKey.Key,
		"key":                  obj.Identifier,
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
	objName := fmt.Sprintf("%s.%d", uploadID, partNumber)
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
	// TODO(barak): insert record associated with upload-id with part etag and number
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
	it := bucket.Objects(a.ctx, &storage.Query{
		Prefix:    qualifiedKey.Key,
		Delimiter: catalog.DefaultPathDelimiter,
	})
	// TODO(barak): query upload id parts - delete and remove records from DB
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

func (a *Adapter) AbortMultiPartUploads(storageNamespace string) error {
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
	translatedUploadID := a.uploadIDTranslator.TranslateUploadID(uploadID)
	lg := a.log().WithFields(logging.Fields{
		"upload_id":            uploadID,
		"translated_upload_id": translatedUploadID,
		"qualified_ns":         qualifiedKey.StorageNamespace,
		"qualified_key":        qualifiedKey.Key,
		"key":                  obj.Identifier,
	})
	// compose one file from all parts
	bucket := a.client.Bucket(qualifiedKey.StorageNamespace)
	srcs := make([]*storage.ObjectHandle, len(multipartList.Part))
	// check input validity
	partList, err := a.multipartTracker.ListMultipartUploadParts(a.ctx, qualifiedKey.StorageNamespace, translatedUploadID)
	if err != nil {
		return nil, 0, err
	}
	// TODO(barak): check length are equal
	for i, p := range multipartList.Part {
		// TODO(barak): check etag and part number for nil first and than compare the values
		var partNumber int64
		if p.PartNumber != nil {
			partNumber = *p.PartNumber
		} else {
			// InvalidPart - BAD REQUEST 400
			return nil, 0, err
		}
		if int(partNumber) != partList[i].Number || *p.ETag != partList[i].ETag {
			// InvalidPart - BAD REQUEST 400
			return nil, 0, err
		}
		objName := fmt.Sprintf("%s.%d", qualifiedKey.Key, partNumber)
		srcs[i] = bucket.Object(objName)
	}

	// TODO(barak): compare parts to the associated upload-id parts in database
	// err = composeMultiPart(qualifiedKey, srcs)
	// WIP
	composer := a.client.
		Bucket(qualifiedKey.StorageNamespace).
		Object(qualifiedKey.Key).
		ComposerFrom(srcs...)
	attrs, err := composer.Run(a.ctx)
	if err != nil {
		lg.WithError(err).Error("CompleteMultipartUpload failed")
		return nil, 0, err
	}
	// TODO(barak): remove upload id tracking from DB
	// delete parts
	for _, src := range srcs {
		if err := src.Delete(a.ctx); err != nil {
			a.log().WithError(err).Warn("Failed to delete multipart upload while compose")
		}
	}
	lg.Debug("completed multipart upload")
	a.uploadIDTranslator.RemoveUploadID(translatedUploadID)
	return &attrs.Etag, attrs.Size, nil
}

// rename composeMultiPart to composeMultipart
/*
func composeMultiPart(srcs []*storage.ObjectHandle) error {
	partsList := srcs
	var nextPartsList []*storage.ObjectHandle
	for len(partsList) > 1 {
		for i := 0; i < len(partsList); i += MaxPartsInCompose {
			partsBatch := make([]*storage.ObjectHandle, MaxPartsInCompose)
			numCopied := copy(partsBatch, partsList[i:])
			partsBatch = partsBatch[:numCopied]
			if numCopied == 1 { // there is a single last element. should be moved to next iteration
				nextPartsList = append(nextPartsList, partsBatch[0])
			}
			// else { // composition may be done in parallel. not first stage
			// perform composition on partsBatch
			// append result to nextPartsList
			// }
		}
		partsList = nextPartsList
		// partsList = partsList[:0]
	}
	return nil
}
*/

func (a *Adapter) ValidateConfiguration(_ string) error {
	return nil
}

func (a *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string) (block.Inventory, error) {
	return nil, fmt.Errorf("inventory %w", ErrNotImplemented)
}
