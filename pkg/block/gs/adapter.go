package gs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/api/iterator"
)

const (
	MaxMultipartObjects = 10000

	delimiter    = "/"
	partSuffix   = ".part_"
	markerSuffix = ".multipart"
)

var (
	ErrNotImplemented      = errors.New("not implemented")
	ErrMismatchPartETag    = errors.New("mismatch part ETag")
	ErrMismatchPartName    = errors.New("mismatch part name")
	ErrMaxMultipartObjects = errors.New("maximum multipart object reached")
	ErrPartListMismatch    = errors.New("multipart part list mismatch")
	ErrMissingTargetAttrs  = errors.New("missing target attributes")
)

type Adapter struct {
	client           *storage.Client
	preSignedExpiry  time.Duration
	disablePreSigned bool
}

func WithPreSignedExpiry(v time.Duration) func(a *Adapter) {
	return func(a *Adapter) {
		if v == 0 {
			a.preSignedExpiry = block.DefaultPreSignExpiryDuration
		} else {
			a.preSignedExpiry = v
		}
	}
}

func WithDisablePreSigned(b bool) func(a *Adapter) {
	return func(a *Adapter) {
		if b {
			a.disablePreSigned = false
		}
	}
}

func NewAdapter(client *storage.Client, opts ...func(adapter *Adapter)) *Adapter {
	a := &Adapter{
		client:          client,
		preSignedExpiry: block.DefaultPreSignExpiryDuration,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *Adapter) log(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx)
}

func (a *Adapter) newPreSignedTime() time.Time {
	return time.Now().UTC().Add(a.preSignedExpiry)
}

func (a *Adapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, _ block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)
	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return err
	}
	w := a.client.Bucket(bucket).Object(key).NewWriter(ctx)
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

func (a *Adapter) Get(ctx context.Context, obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("Get", time.Now(), nil, &err)
	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	r, err := a.client.Bucket(bucket).Object(key).NewReader(ctx)
	if isErrNotFound(err) {
		return nil, block.ErrDataNotFound
	}
	if err != nil {
		a.log(ctx).WithError(err).Errorf("failed to get object bucket %s key %s", bucket, key)
		return nil, err
	}
	return r, nil
}

func (a *Adapter) GetWalker(uri *url.URL) (block.Walker, error) {
	if err := block.ValidateStorageType(uri, block.StorageTypeGS); err != nil {
		return nil, err
	}
	return NewGCSWalker(a.client), nil
}

func (a *Adapter) GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode) (string, error) {
	if a.disablePreSigned {
		return "", block.ErrOperationNotSupported
	}

	var err error
	defer reportMetrics("GetPreSignedURL", time.Now(), nil, &err)

	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return "", err
	}
	method := http.MethodGet
	if mode == block.PreSignModeWrite {
		method = http.MethodPut
	}
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  method,
		Expires: a.newPreSignedTime(),
	}
	k, err := a.client.Bucket(bucket).SignedURL(key, opts)
	if err != nil {
		a.log(ctx).WithError(err).Error("error generating pre-signed URL")
		return "", err
	}
	return k, nil
}

func isErrNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist)
}

func (a *Adapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	var err error
	defer reportMetrics("Exists", time.Now(), nil, &err)
	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return false, err
	}
	_, err = a.client.Bucket(bucket).Object(key).Attrs(ctx)
	if isErrNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (a *Adapter) GetRange(ctx context.Context, obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("GetRange", time.Now(), nil, &err)
	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	r, err := a.client.Bucket(bucket).Object(key).NewRangeReader(ctx, startPosition, endPosition-startPosition+1)
	if isErrNotFound(err) {
		return nil, block.ErrDataNotFound
	}
	if err != nil {
		a.log(ctx).WithError(err).Errorf("failed to get object bucket %s key %s", bucket, key)
		return nil, err
	}
	return r, nil
}

func (a *Adapter) GetProperties(ctx context.Context, obj block.ObjectPointer) (block.Properties, error) {
	var err error
	defer reportMetrics("GetProperties", time.Now(), nil, &err)
	var props block.Properties
	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return props, err
	}
	_, err = a.client.Bucket(bucket).Object(key).Attrs(ctx)
	if err != nil {
		return props, err
	}
	return props, nil
}

func (a *Adapter) Remove(ctx context.Context, obj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Remove", time.Now(), nil, &err)
	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return err
	}
	err = a.client.Bucket(bucket).Object(key).Delete(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).Delete: %w", key, err)
	}
	return nil
}

func (a *Adapter) Copy(ctx context.Context, sourceObj, destinationObj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Copy", time.Now(), nil, &err)
	dstBucket, dstKey, err := a.extractParamsFromObj(destinationObj)
	if err != nil {
		return fmt.Errorf("resolve destination: %w", err)
	}
	srcBucket, srcKey, err := a.extractParamsFromObj(sourceObj)
	if err != nil {
		return fmt.Errorf("resolve source: %w", err)
	}
	destinationObjectHandle := a.client.Bucket(dstBucket).Object(dstKey)
	sourceObjectHandle := a.client.Bucket(srcBucket).Object(srcKey)
	_, err = destinationObjectHandle.CopierFrom(sourceObjectHandle).Run(ctx)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	return nil
}

func (a *Adapter) CreateMultiPartUpload(ctx context.Context, obj block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	var err error
	defer reportMetrics("CreateMultiPartUpload", time.Now(), nil, &err)
	bucket, uploadID, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	// we keep a marker file to identify multipart in progress
	objName := formatMultipartMarkerFilename(uploadID)
	o := a.client.Bucket(bucket).Object(objName)
	w := o.NewWriter(ctx)
	_, err = io.WriteString(w, uploadID)
	if err != nil {
		return nil, fmt.Errorf("io.WriteString: %w", err)
	}
	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("writer.Close: %w", err)
	}
	// log information
	a.log(ctx).WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  bucket,
		"qualified_key": uploadID,
		"key":           obj.Identifier,
	}).Debug("created multipart upload")
	return &block.CreateMultiPartUploadResponse{
		UploadID: uploadID,
	}, nil
}

func (a *Adapter) UploadPart(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), &sizeBytes, &err)
	bucket, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	objName := formatMultipartFilename(uploadID, partNumber)
	o := a.client.Bucket(bucket).Object(objName)
	w := o.NewWriter(ctx)
	_, err = io.Copy(w, reader)
	if err != nil {
		return nil, fmt.Errorf("io.Copy: %w", err)
	}
	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("writer.Close: %w", err)
	}
	attrs, err := o.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("object.Attrs: %w", err)
	}
	return &block.UploadPartResponse{
		ETag: attrs.Etag,
	}, nil
}

func (a *Adapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadCopyPart", time.Now(), nil, &err)
	bucket, _, err := a.extractParamsFromObj(destinationObj)
	if err != nil {
		return nil, err
	}
	objName := formatMultipartFilename(uploadID, partNumber)
	o := a.client.Bucket(bucket).Object(objName)

	srcBucket, srcKey, err := a.extractParamsFromObj(sourceObj)
	if err != nil {
		return nil, fmt.Errorf("resolve source: %w", err)
	}
	sourceObjectHandle := a.client.Bucket(srcBucket).Object(srcKey)

	attrs, err := o.CopierFrom(sourceObjectHandle).Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("CopierFrom: %w", err)
	}
	return &block.UploadPartResponse{
		ETag: attrs.Etag,
	}, nil
}

func (a *Adapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadCopyPartRange", time.Now(), nil, &err)
	bucket, _, err := a.extractParamsFromObj(destinationObj)
	if err != nil {
		return nil, err
	}
	objName := formatMultipartFilename(uploadID, partNumber)
	o := a.client.Bucket(bucket).Object(objName)

	reader, err := a.GetRange(ctx, sourceObj, startPosition, endPosition)
	if err != nil {
		return nil, fmt.Errorf("GetRange: %w", err)
	}
	w := o.NewWriter(ctx)
	_, err = io.Copy(w, reader)
	if err != nil {
		return nil, fmt.Errorf("copy: %w", err)
	}
	err = w.Close()
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("WriterClose: %w", err)
	}
	err = reader.Close()
	if err != nil {
		return nil, fmt.Errorf("ReaderClose: %w", err)
	}

	attrs, err := o.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("object.Attrs: %w", err)
	}
	return &block.UploadPartResponse{
		ETag: attrs.Etag,
	}, nil
}

func (a *Adapter) AbortMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string) error {
	var err error
	defer reportMetrics("AbortMultiPartUpload", time.Now(), nil, &err)
	bucketName, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return err
	}
	bucket := a.client.Bucket(bucketName)

	// delete all related files by listing the prefix
	it := bucket.Objects(ctx, &storage.Query{
		Prefix:    uploadID,
		Delimiter: delimiter,
	})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("bucket(%s).Objects(): %w", bucketName, err)
		}
		if err := bucket.Object(attrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("bucket(%s).object(%s).Delete(): %w", bucketName, attrs.Name, err)
		}
	}
	return nil
}

func (a *Adapter) CompleteMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	bucketName, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	lg := a.log(ctx).WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  bucketName,
		"qualified_key": key,
		"key":           obj.Identifier,
	})

	// list bucket parts and validate request match
	bucketParts, err := a.listMultipartUploadParts(ctx, bucketName, uploadID)
	if err != nil {
		return nil, err
	}
	// validate bucketParts match the request multipartList
	err = a.validateMultipartUploadParts(uploadID, multipartList, bucketParts)
	if err != nil {
		return nil, err
	}

	// prepare names
	parts := make([]string, len(bucketParts))
	for i, part := range bucketParts {
		parts[i] = part.Name
	}

	// compose target object
	targetAttrs, err := a.composeMultipartUploadParts(ctx, bucketName, uploadID, parts)
	if err != nil {
		lg.WithError(err).Error("CompleteMultipartUpload failed")
		return nil, err
	}

	// delete marker
	bucket := a.client.Bucket(bucketName)
	objMarker := bucket.Object(formatMultipartMarkerFilename(uploadID))
	if err := objMarker.Delete(ctx); err != nil {
		a.log(ctx).WithError(err).Warn("Failed to delete multipart upload marker")
	}
	lg.Debug("completed multipart upload")
	return &block.CompleteMultiPartUploadResponse{
		ETag:          targetAttrs.Etag,
		ContentLength: targetAttrs.Size,
	}, nil
}

func (a *Adapter) validateMultipartUploadParts(uploadID string, multipartList *block.MultipartUploadCompletion, bucketParts []*storage.ObjectAttrs) error {
	if len(multipartList.Part) != len(bucketParts) {
		return ErrPartListMismatch
	}
	for i, p := range multipartList.Part {
		objName := formatMultipartFilename(uploadID, p.PartNumber)
		if objName != bucketParts[i].Name {
			return fmt.Errorf("invalid part at position %d: %w", i, ErrMismatchPartName)
		}
		if p.ETag != bucketParts[i].Etag {
			return fmt.Errorf("invalid part at position %d: %w", i, ErrMismatchPartETag)
		}
	}
	return nil
}

func (a *Adapter) listMultipartUploadParts(ctx context.Context, bucketName string, uploadID string) ([]*storage.ObjectAttrs, error) {
	bucket := a.client.Bucket(bucketName)
	var bucketParts []*storage.ObjectAttrs
	it := bucket.Objects(ctx, &storage.Query{
		Delimiter: delimiter,
		Prefix:    uploadID + partSuffix,
	})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing bucket '%s' upload '%s': %w", bucketName, uploadID, err)
		}
		bucketParts = append(bucketParts, attrs)
		if len(bucketParts) > MaxMultipartObjects {
			return nil, fmt.Errorf("listing bucket '%s' upload '%s': %w", bucketName, uploadID, ErrMaxMultipartObjects)
		}
	}
	// sort by name - assume natual sort order
	sort.Slice(bucketParts, func(i, j int) bool {
		return bucketParts[i].Name < bucketParts[j].Name
	})
	return bucketParts, nil
}

func (a *Adapter) composeMultipartUploadParts(ctx context.Context, bucketName string, uploadID string, parts []string) (*storage.ObjectAttrs, error) {
	// compose target from all parts
	bucket := a.client.Bucket(bucketName)
	var targetAttrs *storage.ObjectAttrs
	err := ComposeAll(uploadID, parts, func(target string, parts []string) error {
		objs := make([]*storage.ObjectHandle, len(parts))
		for i := range parts {
			objs[i] = bucket.Object(parts[i])
		}
		// compose target from parts
		attrs, err := bucket.Object(target).ComposerFrom(objs...).Run(ctx)
		if err != nil {
			return err
		}
		if target == uploadID {
			targetAttrs = attrs
		}
		// delete parts
		for _, o := range objs {
			if err := o.Delete(ctx); err != nil {
				a.log(ctx).WithError(err).WithFields(logging.Fields{
					"bucket": bucketName,
					"parts":  parts,
				}).Warn("Failed to delete multipart upload part while compose")
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

func (a *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, fmt.Errorf("inventory %w", ErrNotImplemented)
}

func (a *Adapter) Close() error {
	return a.client.Close()
}

func (a *Adapter) BlockstoreType() string {
	return block.BlockstoreTypeGS
}

func (a *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	info := block.DefaultStorageNamespaceInfo(block.BlockstoreTypeGS)
	if a.disablePreSigned {
		info.PreSignSupport = false
	}
	return info
}

func (a *Adapter) extractParamsFromObj(obj block.ObjectPointer) (string, string, error) {
	qk, err := a.ResolveNamespace(obj.StorageNamespace, obj.Identifier, obj.IdentifierType)
	if err != nil {
		return "", "", err
	}
	bucket, prefix, _ := strings.Cut(qk.GetStorageNamespace(), "/")
	key := qk.GetKey()
	if len(prefix) > 0 { // Avoid situations where prefix is empty or "/"
		key = prefix + "/" + key
	}
	return bucket, key, nil
}

func (a *Adapter) ResolveNamespace(storageNamespace, key string, identifierType block.IdentifierType) (block.QK, error) {
	qualifiedKey, err := block.ResolveNamespace(storageNamespace, key, identifierType)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.GetStorageType() != block.StorageTypeGS {
		return qualifiedKey, block.ErrInvalidNamespace
	}
	return qualifiedKey, nil
}

func (a *Adapter) RuntimeStats() map[string]string {
	return nil
}

func formatMultipartFilename(uploadID string, partNumber int) string {
	// keep natural sort order with zero padding
	return fmt.Sprintf("%s"+partSuffix+"%05d", uploadID, partNumber)
}

func formatMultipartMarkerFilename(uploadID string) string {
	return uploadID + markerSuffix
}
