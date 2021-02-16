package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
)

var (
	ErrNotImplemented = errors.New("not implemented")
)

const (
	BlockstoreType          = "wasb"
	sizeSuffix              = "_size"
	idSuffix                = "_id"
	_1MiB                   = 1024 * 1024
	MaxBuffers              = 1
	defaultMaxRetryRequests = 0
)

type Adapter struct {
	ctx            context.Context
	pipeline       pipeline.Pipeline
	configurations configurations
	serviceURL     string
}

type configurations struct {
	retryReaderOptions azblob.RetryReaderOptions
}

func NewAdapter(pipeline pipeline.Pipeline, serviceURL string, opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		ctx:            context.Background(),
		serviceURL:     serviceURL,
		pipeline:       pipeline,
		configurations: configurations{retryReaderOptions: azblob.RetryReaderOptions{MaxRetryRequests: defaultMaxRetryRequests}},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		pipeline:   a.pipeline,
		serviceURL: a.serviceURL,
		ctx:        ctx,
	}
}

func resolveNamespace(obj block.ObjectPointer) (block.QualifiedKey, error) {
	qualifiedKey, err := block.ResolveNamespace(obj.StorageNamespace, obj.Identifier)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.StorageType != block.StorageTypeAzure {
		return qualifiedKey, block.ErrInvalidNamespace
	}
	return qualifiedKey, nil
}

func resolveNamespacePrefix(lsOpts block.WalkOpts) (block.QualifiedPrefix, error) {
	qualifiedPrefix, err := block.ResolveNamespacePrefix(lsOpts.StorageNamespace, lsOpts.Prefix)
	if err != nil {
		return qualifiedPrefix, err
	}
	if qualifiedPrefix.StorageType != block.StorageTypeAzure {
		return qualifiedPrefix, block.ErrInvalidNamespace
	}
	return qualifiedPrefix, nil
}

func (a *Adapter) GenerateInventory(ctx context.Context, logger logging.Logger, inventoryURL string, shouldSort bool, prefixes []string) (block.Inventory, error) {
	return nil, fmt.Errorf("inventory %w", ErrNotImplemented)
}

func (a *Adapter) getContainerURL(containerName string) azblob.ContainerURL {
	u, err := url.Parse(fmt.Sprintf("%s/%s", a.serviceURL, containerName))
	if err != nil {
		panic(err)
	}
	return azblob.NewContainerURL(*u, a.pipeline)
}

func translatePutOpts(opts block.PutOpts) azblob.UploadStreamToBlockBlobOptions {
	res := azblob.UploadStreamToBlockBlobOptions{}
	if opts.StorageClass != nil {
		res.BlobAccessTier = azblob.AccessTierType(*opts.StorageClass)
	}
	return res
}

func (a *Adapter) Put(obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) error {
	var err error
	defer reportMetrics("Put", time.Now(), &sizeBytes, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}
	container := a.getContainerURL(qualifiedKey.StorageNamespace)
	blobURL := container.NewBlockBlobURL(qualifiedKey.Key)

	_, err = azblob.UploadStreamToBlockBlob(a.ctx, reader, blobURL, translatePutOpts(opts))
	return err
}

func (a *Adapter) Get(obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("Get", time.Now(), nil, &err)

	return a.Download(obj, 0, azblob.CountToEnd)
}

func (a *Adapter) GetRange(obj block.ObjectPointer, startPosition int64, endPosition int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("GetRange", time.Now(), nil, &err)

	return a.Download(obj, startPosition, endPosition-startPosition+1)
}

func (a *Adapter) Download(obj block.ObjectPointer, offset, count int64) (io.ReadCloser, error) {
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, err
	}
	container := a.getContainerURL(qualifiedKey.StorageNamespace)
	blobURL := container.NewBlobURL(qualifiedKey.Key)

	keyOptions := azblob.ClientProvidedKeyOptions{}
	downloadResponse, err := blobURL.Download(a.ctx, offset, count, azblob.BlobAccessConditions{}, false, keyOptions)

	if err != nil {
		return nil, err
	}
	bodyStream := downloadResponse.Body(a.configurations.retryReaderOptions)
	return bodyStream, nil
}

func (a *Adapter) Walk(walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	var err error
	defer reportMetrics("Walk", time.Now(), nil, &err)

	qualifiedPrefix, err := resolveNamespacePrefix(walkOpt)
	if err != nil {
		return err
	}

	containerURL := a.getContainerURL(qualifiedPrefix.StorageNamespace)

	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := containerURL.ListBlobsFlatSegment(a.ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: qualifiedPrefix.Prefix})
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

func (a *Adapter) Exists(obj block.ObjectPointer) (bool, error) {
	var err error
	defer reportMetrics("Exists", time.Now(), nil, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return false, err
	}

	container := a.getContainerURL(qualifiedKey.StorageNamespace)
	blobURL := container.NewBlobURL(qualifiedKey.Key)

	_, err = blobURL.GetProperties(a.ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	var storageErr azblob.StorageError

	if errors.As(err, &storageErr) && storageErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (a *Adapter) GetProperties(obj block.ObjectPointer) (block.Properties, error) {
	var err error
	defer reportMetrics("GetProperties", time.Now(), nil, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return block.Properties{}, err
	}

	container := a.getContainerURL(qualifiedKey.StorageNamespace)
	blobURL := container.NewBlobURL(qualifiedKey.Key)

	props, err := blobURL.GetProperties(a.ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return block.Properties{}, err
	}
	storageClass := props.AccessTier()
	return block.Properties{StorageClass: &storageClass}, nil
}

func (a *Adapter) Remove(obj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Remove", time.Now(), nil, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return err
	}

	container := a.getContainerURL(qualifiedKey.StorageNamespace)
	blobURL := container.NewBlobURL(qualifiedKey.Key)

	_, err = blobURL.Delete(a.ctx, "", azblob.BlobAccessConditions{})
	return err
}

func (a *Adapter) Copy(sourceObj, destinationObj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Copy", time.Now(), nil, &err)

	qualifiedDestinationKey, err := resolveNamespace(destinationObj)
	if err != nil {
		return err
	}
	qualifiedSourceKey, err := resolveNamespace(sourceObj)
	if err != nil {
		return err
	}
	sourceContainer := a.getContainerURL(qualifiedSourceKey.StorageNamespace)
	sourceURL := sourceContainer.NewBlobURL(qualifiedSourceKey.Key)

	destinationContainer := a.getContainerURL(qualifiedDestinationKey.StorageNamespace)
	destinationURL := destinationContainer.NewBlobURL(qualifiedDestinationKey.Key)
	_, err = destinationURL.StartCopyFromURL(a.ctx, sourceURL.URL(), azblob.Metadata{}, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{}, azblob.AccessTierNone, azblob.BlobTagsMap{})
	return err
}

func (a *Adapter) CreateMultiPartUpload(obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (string, error) {
	// Azure has no create multipart upload
	var err error
	defer reportMetrics("CreateMultiPartUpload", time.Now(), nil, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return "", err
	}

	return qualifiedKey.Key, nil
}

func (a *Adapter) UploadPart(obj block.ObjectPointer, size int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), nil, &err)

	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return "", err
	}

	container := a.getContainerURL(qualifiedKey.StorageNamespace)
	hashReader := block.NewHashingReader(reader, block.HashFunctionMD5)

	transferManager, err := azblob.NewStaticBuffer(_1MiB, MaxBuffers)
	if err != nil {
		return "", err
	}
	defer transferManager.Close()
	multipartBlockWriter := NewMultipartBlockWriter(hashReader, container, qualifiedKey.Key)
	_, err = copyFromReader(a.ctx, hashReader, multipartBlockWriter, azblob.UploadStreamToBlockBlobOptions{
		TransferManager: transferManager,
	})
	if err != nil {
		return "", err
	}
	return multipartBlockWriter.etag, nil
}

func (a *Adapter) UploadCopyPart(sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int64) (string, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), nil, &err)

	return a.copyPartRange(sourceObj, destinationObj, 0, azblob.CountToEnd)
}

func (a *Adapter) UploadCopyPartRange(sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber, startPosition, endPosition int64) (string, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), nil, &err)

	return a.copyPartRange(sourceObj, destinationObj, startPosition, endPosition-startPosition+1)
}

func (a *Adapter) copyPartRange(sourceObj, destinationObj block.ObjectPointer, startPosition, count int64) (string, error) {
	qualifiedSourceKey, err := resolveNamespace(sourceObj)
	if err != nil {
		return "", err
	}

	qualifiedDestinationKey, err := resolveNamespace(destinationObj)
	if err != nil {
		return "", err
	}

	destinationContainer := a.getContainerURL(qualifiedDestinationKey.StorageNamespace)
	sourceContainer := a.getContainerURL(qualifiedSourceKey.StorageNamespace)
	sourceBlobURL := sourceContainer.NewBlockBlobURL(qualifiedSourceKey.Key)

	return copyPartRange(a.ctx, destinationContainer, qualifiedDestinationKey.Key, sourceBlobURL, startPosition, count)
}

func (a *Adapter) AbortMultiPartUpload(_ block.ObjectPointer, _ string) error {
	// Azure has no abort, in case of commit, uncommitted parts are erased, otherwise staged data is erased after 7 days
	return nil
}

func (a *Adapter) ValidateConfiguration(_ string) error {
	return nil
}

func (a *Adapter) BlockstoreType() string {
	return BlockstoreType
}

func (a *Adapter) CompleteMultiPartUpload(obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {
	var err error
	defer reportMetrics("CompleteMultiPartUpload", time.Now(), nil, &err)
	qualifiedKey, err := resolveNamespace(obj)
	if err != nil {
		return nil, 0, err
	}
	containerURL := a.getContainerURL(qualifiedKey.StorageNamespace)

	return CompleteMultipart(a.ctx, multipartList.Part, containerURL, qualifiedKey.Key, a.configurations.retryReaderOptions)
}
