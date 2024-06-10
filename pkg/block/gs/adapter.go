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
	ErrMismatchPartETag    = errors.New("mismatch part ETag")
	ErrMismatchPartName    = errors.New("mismatch part name")
	ErrMaxMultipartObjects = errors.New("maximum multipart object reached")
	ErrPartListMismatch    = errors.New("multipart part list mismatch")
	ErrMissingTargetAttrs  = errors.New("missing target attributes")
)

type Adapter struct {
	client                               *storage.Client
	preSignedExpiry                      time.Duration
	disablePreSigned                     bool
	disablePreSignedUI                   bool
	ServerSideEncryptionCustomerSupplied []byte
	ServerSideEncryptionKmsKeyID         string
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
			a.disablePreSigned = true
		}
	}
}

func WithDisablePreSignedUI(b bool) func(a *Adapter) {
	return func(a *Adapter) {
		if b {
			a.disablePreSignedUI = true
		}
	}
}

type AdapterOption func(a *Adapter)

func NewAdapter(client *storage.Client, opts ...AdapterOption) *Adapter {
	a := &Adapter{
		client:          client,
		preSignedExpiry: block.DefaultPreSignExpiryDuration,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func WithServerSideEncryptionCustomerSupplied(value []byte) func(a *Adapter) {
	return func(a *Adapter) {
		a.ServerSideEncryptionCustomerSupplied = value
	}
}

func WithServerSideEncryptionKmsKeyID(s string) func(a *Adapter) {
	return func(a *Adapter) {
		a.ServerSideEncryptionKmsKeyID = s
	}
}

func (a *Adapter) log(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx)
}

func (a *Adapter) newPreSignedTime() time.Time {
	return time.Now().UTC().Add(a.preSignedExpiry)
}

// withReadHandle returns a corresponding handle for reading object based on the encryption settings.
// Ideally we assume all the objects should be encrypted with AES key at the very beginning
// It may be that some existing objects were created before the key is was introduced, so objects will be examined and checked for encryption.
// Multiple keys for individual objects aren't supported! An error will be generated if an improper key is supplied.
// Keys in KMS will not be validated. You should use a proper service account with permissions to access them.
func (o *storageObjectHandle) withReadHandle(ctx context.Context, a *Adapter) *storageObjectHandle {
	att, err := o.Attrs(ctx)
	if err == nil {
		a.log(ctx).Debug("object has attribute customerKeySHA256 means it is encrypted by Customer-Supplied key: ", att.CustomerKeySHA256)
		if a.ServerSideEncryptionCustomerSupplied != nil && att.CustomerKeySHA256 != "" {
			o.ObjectHandle = o.Key(a.ServerSideEncryptionCustomerSupplied)
		}
	} else {
		// Assume no decryption needed when attrs is not found
		a.log(ctx).Debugf("object Attrs get error %w", err)
	}
	return o
}

type storageObjectHandle struct {
	*storage.ObjectHandle
}

func (o *storageObjectHandle) withWriteHandle(a *Adapter) *storageObjectHandle {
	if a.ServerSideEncryptionCustomerSupplied != nil {
		o.ObjectHandle = o.Key(a.ServerSideEncryptionCustomerSupplied)
	}
	return o
}
func (o *storageObjectHandle) newWriter(ctx context.Context, a *Adapter) *storage.Writer {
	w := o.NewWriter(ctx)
	if a.ServerSideEncryptionKmsKeyID != "" {
		w.KMSKeyName = a.ServerSideEncryptionKmsKeyID
	}
	return w
}

func (o *storageObjectHandle) newCopier(a *Adapter, src *storage.ObjectHandle) *storage.Copier {
	c := o.CopierFrom(src)
	if a.ServerSideEncryptionKmsKeyID != "" {
		c.DestinationKMSKeyName = a.ServerSideEncryptionKmsKeyID
	}
	return c
}

func (o *storageObjectHandle) newComposer(a *Adapter, srcs ...*storage.ObjectHandle) *storage.Composer {
	c := o.ComposerFrom(srcs...)
	if a.ServerSideEncryptionKmsKeyID != "" {
		c.KMSKeyName = a.ServerSideEncryptionKmsKeyID
	}
	return c
}

func (a *Adapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, _ block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)
	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return err
	}
	h := storageObjectHandle{a.client.Bucket(bucket).Object(key)}
	w := h.withWriteHandle(a).newWriter(ctx, a)
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

func (a *Adapter) Get(ctx context.Context, obj block.ObjectPointer) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("Get", time.Now(), nil, &err)
	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	h := storageObjectHandle{a.client.Bucket(bucket).Object(key)}
	objHandle := h.withReadHandle(ctx, a)
	r, err := objHandle.NewReader(ctx)
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

var errPreSignedURLWithCSEKNotSupportedError = errors.New("currently PreSignedURL with Customer-supplied encryption key " +
	"is not supported because the key in the config file must be exposed to user")

func errPreSignedURLWithCSEKNotSupported() error {
	return errPreSignedURLWithCSEKNotSupportedError
}

func (a *Adapter) GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode) (string, time.Time, error) {
	if a.disablePreSigned {
		return "", time.Time{}, block.ErrOperationNotSupported
	}

	var err error
	defer reportMetrics("GetPreSignedURL", time.Now(), nil, &err)

	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return "", time.Time{}, err
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
	bucketHandle := a.client.Bucket(bucket)
	att, err := bucketHandle.Object(key).Attrs(ctx)
	if err == nil && att.CustomerKeySHA256 != "" {
		return "", time.Time{}, errPreSignedURLWithCSEKNotSupported()
	}
	k, err := bucketHandle.SignedURL(key, opts)
	if err != nil {
		a.log(ctx).WithError(err).Error("error generating pre-signed URL")
		return "", time.Time{}, err
	}
	// TODO(#6347): Report expiry.
	return k, time.Time{}, nil
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
	h := storageObjectHandle{a.client.Bucket(bucket).Object(key)}
	objHandle := h.withReadHandle(ctx, a)
	r, err := objHandle.NewRangeReader(ctx, startPosition, endPosition-startPosition+1)
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
	dstHandle := &storageObjectHandle{a.client.Bucket(dstBucket).Object(dstKey)}
	dstHandle = dstHandle.withWriteHandle(a)
	srcHandle := &storageObjectHandle{a.client.Bucket(srcBucket).Object(srcKey)}
	srcHandle = srcHandle.withReadHandle(ctx, a)
	copier := dstHandle.newCopier(a, srcHandle.ObjectHandle)
	_, err = copier.Run(ctx)
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
	h := storageObjectHandle{a.client.Bucket(bucket).Object(objName)}
	w := h.withWriteHandle(a).newWriter(ctx, a)
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
	h := storageObjectHandle{a.client.Bucket(bucket).Object(objName)}
	w := h.withWriteHandle(a).newWriter(ctx, a)
	_, err = io.Copy(w, reader)
	if err != nil {
		return nil, fmt.Errorf("io.Copy: %w", err)
	}
	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("writer.Close: %w", err)
	}
	attrs, err := h.Attrs(ctx)
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

	srcBucket, srcKey, err := a.extractParamsFromObj(sourceObj)
	if err != nil {
		return nil, fmt.Errorf("resolve source: %w", err)
	}

	srcHandle := &storageObjectHandle{a.client.Bucket(srcBucket).Object(srcKey)}
	srcHandle = srcHandle.withReadHandle(ctx, a)
	h := storageObjectHandle{a.client.Bucket(bucket).Object(objName)}
	copier := h.withWriteHandle(a).newCopier(a, srcHandle.ObjectHandle)
	attrs, err := copier.Run(ctx)
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

	reader, err := a.GetRange(ctx, sourceObj, startPosition, endPosition)
	if err != nil {
		return nil, fmt.Errorf("GetRange: %w", err)
	}
	h := storageObjectHandle{a.client.Bucket(bucket).Object(objName)}
	w := h.withWriteHandle(a).newWriter(ctx, a)
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

	attrs, err := h.Attrs(ctx)
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
		return fmt.Errorf("part list mismatch - expected %d parts, got %d: %w", len(bucketParts), len(multipartList.Part), ErrPartListMismatch)
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
			h := storageObjectHandle{bucket.Object(parts[i])}
			objs[i] = h.withReadHandle(ctx, a).ObjectHandle
		}
		// compose target from parts
		h := storageObjectHandle{bucket.Object(target)}
		composer := h.withWriteHandle(a).newComposer(a, objs...)
		attrs, err := composer.Run(ctx)
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
	if !(a.disablePreSignedUI || a.disablePreSigned) {
		info.PreSignSupportUI = true
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

func (a *Adapter) ResolveNamespace(storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	qualifiedKey, err := block.DefaultResolveNamespace(storageNamespace, key, identifierType)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.GetStorageType() != block.StorageTypeGS {
		return qualifiedKey, fmt.Errorf("expected storage type gs: %w", block.ErrInvalidAddress)
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

func (a *Adapter) GetPresignUploadPartURL(_ context.Context, _ block.ObjectPointer, _ string, _ int) (string, error) {
	return "", block.ErrOperationNotSupported
}

func (a *Adapter) ListParts(_ context.Context, _ block.ObjectPointer, _ string, _ block.ListPartsOpts) (*block.ListPartsResponse, error) {
	return nil, block.ErrOperationNotSupported
}
