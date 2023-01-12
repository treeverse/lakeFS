package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	ErrNotImplemented = errors.New("not implemented")
	ErrAsyncCopy      = errors.New("asynchronous copy not supported")
)

const (
	sizeSuffix              = "_size"
	idSuffix                = "_id"
	_1MiB                   = 1024 * 1024
	MaxBuffers              = 1
	defaultMaxRetryRequests = 0
	AuthMethodAccessKey     = "access-key"
	AuthMethodMSI           = "msi"

	preSignedBlobPattern = "https://%s.blob.core.windows.net/%s/%s?%s"
)

type Adapter struct {
	pipeline                      pipeline.Pipeline
	configurations                configurations
	credentials                   azblob.Credential
	preSignedURLDurationGenerator func() time.Time
	keyCredentials                *azblob.SharedKeyCredential
}

type configurations struct {
	retryReaderOptions azblob.RetryReaderOptions
}

func WithPreSignedURLDurationGenerator(f func() time.Time) func(a *Adapter) {
	return func(a *Adapter) {
		a.preSignedURLDurationGenerator = f
	}
}

func NewAdapter(pipeline pipeline.Pipeline, credentials azblob.Credential, opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		pipeline:       pipeline,
		credentials:    credentials,
		configurations: configurations{retryReaderOptions: azblob.RetryReaderOptions{MaxRetryRequests: defaultMaxRetryRequests}},
		preSignedURLDurationGenerator: func() time.Time {
			return time.Now().UTC().Add(block.DefaultPreSignExpiryDuration)
		},
	}
	keyCredentials, ok := credentials.(*azblob.SharedKeyCredential)
	if ok {
		a.keyCredentials = keyCredentials
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

type BlobURLInfo struct {
	ContainerURL  string
	ContainerName string
	BlobURL       string
}

type PrefixURLInfo struct {
	ContainerURL string
	Prefix       string
}

func resolveBlobURLInfoFromURL(pathURL *url.URL) (BlobURLInfo, error) {
	var qk BlobURLInfo
	storageType, err := block.GetStorageType(pathURL)
	if err != nil {
		return qk, err
	}
	if storageType != block.StorageTypeAzure {
		return qk, block.ErrInvalidNamespace
	}
	// In azure the first part of the path is part of the storage namespace
	trimmedPath := strings.Trim(pathURL.Path, "/")
	parts := strings.Split(trimmedPath, "/")
	if len(parts) == 0 {
		return qk, block.ErrInvalidNamespace
	}
	return BlobURLInfo{
		ContainerURL:  fmt.Sprintf("%s://%s/%s", pathURL.Scheme, pathURL.Host, parts[0]),
		ContainerName: parts[0],
		BlobURL:       strings.Join(parts[1:], "/"),
	}, nil
}

func resolveBlobURLInfo(obj block.ObjectPointer) (BlobURLInfo, error) {
	key := obj.Identifier
	defaultNamespace := obj.StorageNamespace
	var qk BlobURLInfo
	// check if the key is fully qualified
	parsedKey, err := url.ParseRequestURI(key)
	if err != nil {
		// is not fully qualified, treat as key only
		// if we don't have a trailing slash for the namespace, add it.
		parsedNamespace, err := url.ParseRequestURI(defaultNamespace)
		if err != nil {
			return qk, err
		}
		qp, err := resolveBlobURLInfoFromURL(parsedNamespace)
		if err != nil {
			return qk, err
		}
		info := BlobURLInfo{
			ContainerURL:  qp.ContainerURL,
			ContainerName: qp.ContainerName,
			BlobURL:       qp.BlobURL + "/" + key,
		}
		if qp.BlobURL == "" {
			info.BlobURL = key
		}
		return info, nil
	}
	return resolveBlobURLInfoFromURL(parsedKey)
}

func resolveNamespacePrefix(lsOpts block.WalkOpts) (PrefixURLInfo, error) {
	qualifiedPrefix, err := resolveBlobURLInfo(block.ObjectPointer{
		StorageNamespace: lsOpts.StorageNamespace,
		Identifier:       lsOpts.Prefix,
	})
	if err != nil {
		return PrefixURLInfo{}, err
	}

	return PrefixURLInfo{
		ContainerURL: qualifiedPrefix.ContainerURL,
		Prefix:       qualifiedPrefix.BlobURL,
	}, nil
}

func (a *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, fmt.Errorf("inventory %w", ErrNotImplemented)
}

func (a *Adapter) getContainerURL(rawURL string) azblob.ContainerURL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return azblob.NewContainerURL(*u, a.pipeline)
}

func (a *Adapter) translatePutOpts(ctx context.Context, opts block.PutOpts) azblob.UploadStreamToBlockBlobOptions {
	res := azblob.UploadStreamToBlockBlobOptions{}
	if opts.StorageClass == nil {
		return res
	}

	for _, t := range azblob.PossibleAccessTierTypeValues() {
		if strings.EqualFold(*opts.StorageClass, string(t)) {
			res.BlobAccessTier = t
			break
		}
	}

	if res.BlobAccessTier == "" {
		a.log(ctx).WithField("tier_type", *opts.StorageClass).Warn("Unknown Azure tier type")
	}

	return res
}

func (a *Adapter) log(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx)
}

func (a *Adapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)
	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return err
	}
	container := a.getContainerURL(qualifiedKey.ContainerURL)
	blobURL := container.NewBlockBlobURL(qualifiedKey.BlobURL)

	// TODO(Guys): remove this work around once azure fixes panic issue and use azblob.UploadStreamToBlockBlob
	transferManager, err := azblob.NewStaticBuffer(_1MiB, MaxBuffers)
	if err != nil {
		return err
	}
	uploadOpts := a.translatePutOpts(ctx, opts)
	uploadOpts.TransferManager = transferManager
	defer transferManager.Close()
	resp, err := copyFromReader(ctx, reader, blobURL, uploadOpts)
	if err != nil {
		return err
	}
	_ = resp == nil // this is done in order to ignore "result 0 is never used" error ( copyFromReader is copied from azure, and we want to keep it with minimum changes)
	return nil
}

func (a *Adapter) Get(ctx context.Context, obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("Get", time.Now(), nil, &err)

	return a.Download(ctx, obj, 0, azblob.CountToEnd)
}

func (a *Adapter) GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode) (string, error) {
	permissions := azblob.BlobSASPermissions{Read: true}
	if mode == block.PreSignModeWrite {
		permissions = azblob.BlobSASPermissions{Write: true}
	}
	return a.getPreSignedURL(ctx, obj, permissions)
}

func (a *Adapter) getPreSignedURL(ctx context.Context, obj block.ObjectPointer, permissions azblob.BlobSASPermissions) (string, error) {
	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return "", err
	}
	vals := azblob.BlobSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS, // Users MUST use HTTPS (not HTTP)
		ExpiryTime:    a.preSignedURLDurationGenerator(),
		ContainerName: qualifiedKey.ContainerName,
		BlobName:      qualifiedKey.BlobURL,
		Permissions:   permissions.String(),
	}

	if a.keyCredentials == nil {
		err = fmt.Errorf("pre-signed mode on Azure is only supported for shared key credentials: %w", ErrNotImplemented)
		a.log(ctx).WithError(err).Error("no support for pre-signed using this Azure credentials provider")
		return "", err
	}

	sasQueryParams, err := vals.NewSASQueryParameters(a.keyCredentials)
	if err != nil {
		a.log(ctx).WithError(err).Error("could not generate SAS query parameters")
		return "", err
	}
	// Create the URL of the resource you wish to access and append the SAS query parameters.
	// Since this is a blob SAS, the URL is to the Azure storage blob.
	qp := sasQueryParams.Encode()
	return fmt.Sprintf(preSignedBlobPattern,
		a.keyCredentials.AccountName(), qualifiedKey.ContainerName, qualifiedKey.BlobURL, qp), nil
}

func (a *Adapter) GetRange(ctx context.Context, obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("GetRange", time.Now(), nil, &err)

	return a.Download(ctx, obj, startPosition, endPosition-startPosition+1)
}

func (a *Adapter) Download(ctx context.Context, obj block.ObjectPointer, offset, count int64) (io.ReadCloser, error) {
	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return nil, err
	}
	container := a.getContainerURL(qualifiedKey.ContainerURL)
	blobURL := container.NewBlobURL(qualifiedKey.BlobURL)

	keyOptions := azblob.ClientProvidedKeyOptions{}
	downloadResponse, err := blobURL.Download(ctx, offset, count, azblob.BlobAccessConditions{}, false, keyOptions)
	if isErrNotFound(err) {
		return nil, block.ErrDataNotFound
	}
	if err != nil {
		a.log(ctx).WithError(err).Errorf("failed to get azure blob from container %s key %s", container, blobURL)
		return nil, err
	}
	bodyStream := downloadResponse.Body(a.configurations.retryReaderOptions)
	return bodyStream, nil
}

func (a *Adapter) Walk(ctx context.Context, walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	var err error
	defer reportMetrics("Walk", time.Now(), nil, &err)

	qualifiedPrefix, err := resolveNamespacePrefix(walkOpt)
	if err != nil {
		return err
	}

	containerURL := a.getContainerURL(qualifiedPrefix.ContainerURL)

	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: qualifiedPrefix.Prefix})
		if err != nil {
			return err
		}
		marker = listBlob.NextMarker
		for _, blobInfo := range listBlob.Segment.BlobItems {
			if err := walkFn(blobInfo.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func isErrNotFound(err error) bool {
	var storageErr azblob.StorageError
	return errors.As(err, &storageErr) && storageErr.ServiceCode() == azblob.ServiceCodeBlobNotFound
}

func (a *Adapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	var err error
	defer reportMetrics("Exists", time.Now(), nil, &err)

	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return false, err
	}

	container := a.getContainerURL(qualifiedKey.ContainerURL)
	blobURL := container.NewBlobURL(qualifiedKey.BlobURL)

	_, err = blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})

	if isErrNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (a *Adapter) GetProperties(ctx context.Context, obj block.ObjectPointer) (block.Properties, error) {
	var err error
	defer reportMetrics("GetProperties", time.Now(), nil, &err)

	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return block.Properties{}, err
	}

	container := a.getContainerURL(qualifiedKey.ContainerURL)
	blobURL := container.NewBlobURL(qualifiedKey.BlobURL)

	props, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return block.Properties{}, err
	}
	storageClass := props.AccessTier()
	return block.Properties{StorageClass: &storageClass}, nil
}

func (a *Adapter) Remove(ctx context.Context, obj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Remove", time.Now(), nil, &err)

	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return err
	}

	container := a.getContainerURL(qualifiedKey.ContainerURL)
	blobURL := container.NewBlobURL(qualifiedKey.BlobURL)

	_, err = blobURL.Delete(ctx, "", azblob.BlobAccessConditions{})
	return err
}

func (a *Adapter) Copy(ctx context.Context, sourceObj, destinationObj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Copy", time.Now(), nil, &err)

	qualifiedDestinationKey, err := resolveBlobURLInfo(destinationObj)
	if err != nil {
		return err
	}
	qualifiedSourceKey, err := resolveBlobURLInfo(sourceObj)
	if err != nil {
		return err
	}
	sourceContainer := a.getContainerURL(qualifiedSourceKey.ContainerURL)
	sourceURL := sourceContainer.NewBlobURL(qualifiedSourceKey.BlobURL)

	destinationContainer := a.getContainerURL(qualifiedDestinationKey.ContainerURL)
	destinationURL := destinationContainer.NewBlobURL(qualifiedDestinationKey.BlobURL)
	resp, err := destinationURL.StartCopyFromURL(ctx, sourceURL.URL(), azblob.Metadata{}, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{}, azblob.AccessTierNone, azblob.BlobTagsMap{})
	if err != nil {
		return err
	}
	// validate copy is not asynchronous
	copyStatus := resp.CopyStatus()
	if copyStatus == "pending" {
		return ErrAsyncCopy
	}
	return nil
}

func (a *Adapter) CreateMultiPartUpload(_ context.Context, obj block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	// Azure has no create multipart upload
	var err error
	defer reportMetrics("CreateMultiPartUpload", time.Now(), nil, &err)

	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return nil, err
	}

	return &block.CreateMultiPartUploadResponse{
		UploadID: qualifiedKey.BlobURL,
	}, nil
}

func (a *Adapter) UploadPart(ctx context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, _ string, _ int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), nil, &err)

	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return nil, err
	}

	container := a.getContainerURL(qualifiedKey.ContainerURL)
	hashReader := block.NewHashingReader(reader, block.HashFunctionMD5)

	transferManager, err := azblob.NewStaticBuffer(_1MiB, MaxBuffers)
	if err != nil {
		return nil, err
	}
	defer transferManager.Close()
	multipartBlockWriter := NewMultipartBlockWriter(hashReader, container, qualifiedKey.BlobURL)
	_, err = copyFromReader(ctx, hashReader, multipartBlockWriter, azblob.UploadStreamToBlockBlobOptions{
		TransferManager: transferManager,
	})
	if err != nil {
		return nil, err
	}
	return &block.UploadPartResponse{
		ETag: strings.Trim(multipartBlockWriter.etag, `"`),
	}, nil
}

func (a *Adapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, _ string, _ int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), nil, &err)

	return a.copyPartRange(ctx, sourceObj, destinationObj, 0, azblob.CountToEnd)
}

func (a *Adapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, _ string, _ int, startPosition, endPosition int64) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), nil, &err)
	return a.copyPartRange(ctx, sourceObj, destinationObj, startPosition, endPosition-startPosition+1)
}

func (a *Adapter) copyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, startPosition, count int64) (*block.UploadPartResponse, error) {
	qualifiedSourceKey, err := resolveBlobURLInfo(sourceObj)
	if err != nil {
		return nil, err
	}

	qualifiedDestinationKey, err := resolveBlobURLInfo(destinationObj)
	if err != nil {
		return nil, err
	}

	destinationContainer := a.getContainerURL(qualifiedDestinationKey.ContainerURL)
	sourceContainer := a.getContainerURL(qualifiedSourceKey.ContainerURL)
	sourceBlobURL := sourceContainer.NewBlockBlobURL(qualifiedSourceKey.BlobURL)

	return copyPartRange(ctx, destinationContainer, qualifiedDestinationKey.BlobURL, sourceBlobURL, startPosition, count)
}

func (a *Adapter) AbortMultiPartUpload(_ context.Context, _ block.ObjectPointer, _ string) error {
	// Azure has no abort, in case of commit, uncommitted parts are erased, otherwise staged data is erased after 7 days
	return nil
}

func (a *Adapter) BlockstoreType() string {
	return block.BlockstoreTypeAzure
}

func (a *Adapter) CompleteMultiPartUpload(ctx context.Context, obj block.ObjectPointer, _ string, multipartList *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return nil, err
	}
	containerURL := a.getContainerURL(qualifiedKey.ContainerURL)
	return completeMultipart(ctx, multipartList.Part, containerURL, qualifiedKey.BlobURL, a.configurations.retryReaderOptions)
}

func (a *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	return block.StorageNamespaceInfo{
		ValidityRegex: `^https?://`,
		Example:       "https://mystorageaccount.blob.core.windows.net/mycontainer/",
	}
}

func (a *Adapter) RuntimeStats() map[string]string {
	return nil
}
