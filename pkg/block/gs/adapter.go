package gs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
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
	ErrInvalidPartName     = errors.New("invalid part name")
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

func (a *Adapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, _ block.PutOpts) (*block.PutResponse, error) {
	var err error
	defer reportMetrics("Put", obj.StorageID, time.Now(), &sizeBytes, &err)
	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	h := storageObjectHandle{a.client.Bucket(bucket).Object(key)}
	w := h.withWriteHandle(a).newWriter(ctx, a)
	_, err = io.Copy(w, reader)
	if err != nil {
		return nil, fmt.Errorf("io.Copy: %w", err)
	}
	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("writer.Close: %w", err)
	}
	return &block.PutResponse{}, nil
}

func (a *Adapter) Get(ctx context.Context, obj block.ObjectPointer) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("Get", obj.StorageID, time.Now(), nil, &err)
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

func (a *Adapter) GetWalker(_ string, opts block.WalkerOptions) (block.Walker, error) {
	if err := block.ValidateStorageType(opts.StorageURI, block.StorageTypeGS); err != nil {
		return nil, err
	}
	return NewGCSWalker(a.client), nil
}

var errPreSignedURLWithCSEKNotSupportedError = errors.New("currently PreSignedURL with Customer-supplied encryption key " +
	"is not supported because the key in the config file must be exposed to user")

func errPreSignedURLWithCSEKNotSupported() error {
	return errPreSignedURLWithCSEKNotSupportedError
}

func (a *Adapter) GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode, filename string) (string, time.Time, error) {
	if a.disablePreSigned {
		return "", time.Time{}, block.ErrOperationNotSupported
	}

	var err error
	defer reportMetrics("GetPreSignedURL", obj.StorageID, time.Now(), nil, &err)

	bucket, key, err := a.extractParamsFromObj(obj)
	if err != nil {
		return "", time.Time{}, err
	}

	bucketHandle := a.client.Bucket(bucket)
	att, err := bucketHandle.Object(key).Attrs(ctx)
	if err == nil && att.CustomerKeySHA256 != "" {
		return "", time.Time{}, errPreSignedURLWithCSEKNotSupported()
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

	// Add content-disposition if filename provided
	if mode == block.PreSignModeRead && filename != "" {
		contentDisposition := mime.FormatMediaType("attachment", map[string]string{
			"filename": path.Base(filename),
		})
		if contentDisposition != "" {
			opts.QueryParameters = url.Values{
				"response-content-disposition": {contentDisposition},
			}
		}
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
	defer reportMetrics("Exists", obj.StorageID, time.Now(), nil, &err)
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
	defer reportMetrics("GetRange", obj.StorageID, time.Now(), nil, &err)
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
	defer reportMetrics("GetProperties", obj.StorageID, time.Now(), nil, &err)
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
	defer reportMetrics("Remove", obj.StorageID, time.Now(), nil, &err)
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
	defer reportMetrics("Copy", sourceObj.StorageID, time.Now(), nil, &err)
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
	defer reportMetrics("CreateMultiPartUpload", obj.StorageID, time.Now(), nil, &err)
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
	defer reportMetrics("UploadPart", obj.StorageID, time.Now(), &sizeBytes, &err)
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
		ETag: hex.EncodeToString(attrs.MD5),
	}, nil
}

func (a *Adapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadCopyPart", sourceObj.StorageID, time.Now(), nil, &err)
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
		ETag: hex.EncodeToString(attrs.MD5),
	}, nil
}

func (a *Adapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int, startPosition, endPosition int64) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadCopyPartRange", sourceObj.StorageID, time.Now(), nil, &err)
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
		ETag: hex.EncodeToString(attrs.MD5),
	}, nil
}

func (a *Adapter) AbortMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string) error {
	var err error
	defer reportMetrics("AbortMultiPartUpload", obj.StorageID, time.Now(), nil, &err)
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
	defer reportMetrics("CompleteMultiPartUpload", obj.StorageID, time.Now(), nil, &err)
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
	lg.Debug("started multipart upload")

	parts, err := a.getPartNamesWithValidation(ctx, bucketName, uploadID, multipartList)
	if err != nil {
		return nil, err
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
		// composite does not return MD5 - it's OK because S3 complete multipart upload does not return MD5 either
		ETag:          targetAttrs.Etag,
		MTime:         &targetAttrs.Created,
		ContentLength: targetAttrs.Size,
	}, nil
}

func (a *Adapter) getPartNamesWithValidation(ctx context.Context, bucketName, uploadID string, multipartList *block.MultipartUploadCompletion) ([]string, error) {
	// list bucket parts and validate request match
	bucketParts, _, err := a.listMultipartUploadParts(ctx, bucketName, uploadID, block.ListPartsOpts{})
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
	return parts, nil
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
		// client ETag == GCS MD5
		if p.ETag != hex.EncodeToString(bucketParts[i].MD5) {
			return fmt.Errorf("invalid part at position %d: %w", i, ErrMismatchPartETag)
		}
	}
	return nil
}

// listMultipartUploadParts retrieves the parts of a multipart upload for a given bucket and upload ID.
// It supports filtering and pagination using the provided options.
// It returns a slice of ObjectAttrs representing the parts, a continuation token for pagination, and any error encountered.
func (a *Adapter) listMultipartUploadParts(ctx context.Context, bucketName string, uploadID string, opts block.ListPartsOpts) ([]*storage.ObjectAttrs, *string, error) {
	bucket := a.client.Bucket(bucketName)
	var bucketParts []*storage.ObjectAttrs
	query := &storage.Query{
		Delimiter: delimiter,
		Prefix:    uploadID + partSuffix,
	}
	if opts.PartNumberMarker != nil {
		query.StartOffset = *opts.PartNumberMarker
	}
	var partNumberMarker *string
	err := query.SetAttrSelection([]string{"Name", "Etag", "MD5"})
	if err != nil {
		return nil, nil, err
	}
	it := bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("listing bucket '%s' upload '%s': %w", bucketName, uploadID, err)
		}

		// filter out invalid part names
		attrsName := attrs.Name
		if !isPartName(attrsName) {
			continue
		}

		if len(bucketParts) > MaxMultipartObjects {
			return nil, nil, fmt.Errorf("listing bucket '%s' upload '%s': %w", bucketName, uploadID, ErrMaxMultipartObjects)
		}
		if opts.MaxParts != nil && len(bucketParts) >= int(*opts.MaxParts) {
			partNumberMarker = &attrsName
			break
		}
		bucketParts = append(bucketParts, attrs)
	}
	// sort by name - assume natural sort order
	sort.Slice(bucketParts, func(i, j int) bool {
		return bucketParts[i].Name < bucketParts[j].Name
	})
	return bucketParts, partNumberMarker, nil
}

// isPartName checks it's a valid part name, as opposed to an already merged group of parts
func isPartName(name string) bool {
	if len(name) < len(partSuffix)+5 {
		return false
	}
	suffixSubstring := name[len(name)-5-len(partSuffix) : len(name)-5]
	return partSuffix == suffixSubstring
}

func (a *Adapter) composeMultipartUploadParts(ctx context.Context, bucketName string, uploadID string, parts []string) (*storage.ObjectAttrs, error) {
	// compose target from all parts
	bucket := a.client.Bucket(bucketName)
	targetAttrs, err := ComposeAll(uploadID, parts, func(target string, parts []string) (*storage.ObjectAttrs, error) {
		objs := make([]*storage.ObjectHandle, len(parts))
		for i := range parts {
			h := storageObjectHandle{bucket.Object(parts[i])}
			objs[i] = h.withReadHandle(ctx, a).ObjectHandle
		}
		// compose target from parts
		targetHandle := storageObjectHandle{bucket.Object(target)}
		composer := targetHandle.withWriteHandle(a).newComposer(a, objs...)
		attrs, err := composer.Run(ctx)
		if err != nil {
			return nil, err
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
		return attrs, nil
	})
	if err == nil && targetAttrs == nil {
		return nil, ErrMissingTargetAttrs
	}
	return targetAttrs, err
}

func (a *Adapter) Close() error {
	return a.client.Close()
}

func (a *Adapter) BlockstoreType() string {
	return block.BlockstoreTypeGS
}

func (a *Adapter) BlockstoreMetadata(_ context.Context) (*block.BlockstoreMetadata, error) {
	return nil, block.ErrOperationNotSupported
}

func (a *Adapter) GetStorageNamespaceInfo(string) *block.StorageNamespaceInfo {
	info := block.DefaultStorageNamespaceInfo(block.BlockstoreTypeGS)
	if a.disablePreSigned {
		info.PreSignSupport = false
	}
	if !(a.disablePreSignedUI || a.disablePreSigned) {
		info.PreSignSupportUI = true
	}
	return &info
}

func (a *Adapter) extractParamsFromObj(obj block.ObjectPointer) (string, string, error) {
	qk, err := a.ResolveNamespace(obj.StorageID, obj.StorageNamespace, obj.Identifier, obj.IdentifierType)
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

func (a *Adapter) ResolveNamespace(_, storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	qualifiedKey, err := block.DefaultResolveNamespace(storageNamespace, key, identifierType)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.GetStorageType() != block.StorageTypeGS {
		return qualifiedKey, fmt.Errorf("expected storage type gs: %w", block.ErrInvalidAddress)
	}
	return qualifiedKey, nil
}

func (a *Adapter) GetRegion(_ context.Context, _, _ string) (string, error) {
	return "", block.ErrOperationNotSupported
}

func (a *Adapter) RuntimeStats() map[string]string {
	return nil
}

func formatMultipartFilename(uploadID string, partNumber int) string {
	// keep natural sort order with zero padding
	return fmt.Sprintf("%s"+partSuffix+"%05d", uploadID, partNumber)
}

func extractPartNumber(filename string) (int, error) {
	if !isPartName(filename) {
		return 0, fmt.Errorf("invalid part name '%s': %w", filename, ErrInvalidPartName)
	}

	partNumberStr := filename[len(filename)-5:]

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil {
		return 0, fmt.Errorf("invalid part number in filename: %w", err)
	}

	return partNumber, nil
}

func formatMultipartMarkerFilename(uploadID string) string {
	return uploadID + markerSuffix
}

func (a *Adapter) GetPresignUploadPartURL(_ context.Context, _ block.ObjectPointer, _ string, _ int) (string, error) {
	return "", block.ErrOperationNotSupported
}

func (a *Adapter) ListParts(ctx context.Context, obj block.ObjectPointer, uploadID string, opts block.ListPartsOpts) (*block.ListPartsResponse, error) {
	bucketName, _, err := a.extractParamsFromObj(obj)
	if err != nil {
		return nil, err
	}
	// validate uploadID exists
	bucket := a.client.Bucket(bucketName)
	objMarker := bucket.Object(formatMultipartMarkerFilename(uploadID))
	_, err = objMarker.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	bucketParts, nextPartNumberMarker, err := a.listMultipartUploadParts(ctx, bucketName, uploadID, opts)
	if err != nil {
		return nil, err
	}
	parts := make([]block.MultipartPart, len(bucketParts))
	for i, part := range bucketParts {
		partNumber, err := extractPartNumber(part.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to extract part number: %w", err)
		}
		parts[i] = block.MultipartPart{
			// Using MD5 as the ETag - AWS S3 uses MD5 as the ETag, and some clients rely on this behavior to validate that the part ETag matches the MD5 checksum.
			ETag:         hex.EncodeToString(part.MD5),
			PartNumber:   partNumber,
			LastModified: part.Updated,
			Size:         part.Size,
		}
	}
	return &block.ListPartsResponse{
		Parts:                parts,
		NextPartNumberMarker: nextPartNumberMarker,
		IsTruncated:          nextPartNumberMarker != nil,
	}, nil
}

func (a *Adapter) ListMultipartUploads(_ context.Context, _ block.ObjectPointer, _ block.ListMultipartUploadsOpts) (*block.ListMultipartUploadsResponse, error) {
	return nil, block.ErrOperationNotSupported
}
