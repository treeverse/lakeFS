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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
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

	AzURLTemplate = "https://%s.blob.core.windows.net/"
)

type Adapter struct {
	configurations configurations
	client         service.Client
}

type configurations struct {
	retryReaderOptions azblob.RetryReaderOptions
}

func NewAdapter(client service.Client, opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		// pipeline:       client,
		configurations: configurations{retryReaderOptions: azblob.RetryReaderOptions{MaxRetries: defaultMaxRetryRequests}},
		client:         client,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

type BlobURLInfo struct {
	ContainerURL  string
	BlobURL       string
	ContainerName string
}

type PrefixURLInfo struct {
	ContainerURL  string
	ContainerName string
	Prefix        string
}

func ResolveBlobURLInfoFromURL(pathURL *url.URL) (BlobURLInfo, error) {
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
		qp, err := ResolveBlobURLInfoFromURL(parsedNamespace)
		if err != nil {
			return qk, err
		}
		info := BlobURLInfo{
			ContainerURL:  qp.ContainerURL,
			BlobURL:       qp.BlobURL + "/" + key,
			ContainerName: strings.Trim(parsedNamespace.Path, "/"),
		}
		if qp.BlobURL == "" {
			info.BlobURL = key
		}

		return info, nil
	}
	return ResolveBlobURLInfoFromURL(parsedKey)
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
		ContainerURL:  qualifiedPrefix.ContainerURL,
		ContainerName: qualifiedPrefix.ContainerName,
		Prefix:        qualifiedPrefix.BlobURL,
	}, nil
}

func (a *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, fmt.Errorf("inventory %w", ErrNotImplemented)
}

func (a *Adapter) translatePutOpts(ctx context.Context, opts block.PutOpts) azblob.UploadStreamOptions {
	res := azblob.UploadStreamOptions{}
	if opts.StorageClass == nil {
		return res
	}

	for _, t := range blob.PossibleAccessTierValues() {
		if strings.EqualFold(*opts.StorageClass, string(t)) {
			accessTier := t
			res.AccessTier = &accessTier
			break
		}
	}

	if res.AccessTier == nil {
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
	o := a.translatePutOpts(ctx, opts)
	_, err = a.client.NewContainerClient(qualifiedKey.ContainerName).NewBlockBlobClient(qualifiedKey.BlobURL).UploadStream(ctx, reader, &o)
	return err
}

func (a *Adapter) Get(ctx context.Context, obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("Get", time.Now(), nil, &err)

	return a.Download(ctx, obj, 0, blockblob.CountToEnd)
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
	container := a.client.NewContainerClient(qualifiedKey.ContainerName)
	blobURL := container.NewBlockBlobClient(qualifiedKey.BlobURL)

	// keyOptions := service.ClientProvidedKeyOptions{}
	downloadResponse, err := blobURL.DownloadStream(ctx, &azblob.DownloadStreamOptions{
		RangeGetContentMD5: nil,
		Range: blob.HTTPRange{
			Offset: offset,
			Count:  count,
		},
	})
	if isErrNotFound(err) {
		return nil, block.ErrDataNotFound
	}
	if err != nil {
		return nil, err
	}
	return downloadResponse.Body, nil
}

func (a *Adapter) Walk(_ context.Context, walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	var err error
	defer reportMetrics("Walk", time.Now(), nil, &err)

	qualifiedPrefix, err := resolveNamespacePrefix(walkOpt)
	if err != nil {
		return err
	}

	containerURL := a.client.NewContainerClient(qualifiedPrefix.ContainerName)
	var marker *string
	for {
		listBlob := containerURL.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
			Prefix: &qualifiedPrefix.Prefix,
			Marker: marker,
		})

		for listBlob.More() {
			resp, err := listBlob.NextPage(context.Background())
			if err != nil {
				return err
			}
			for _, blobInfo := range resp.Segment.BlobItems {
				if err := walkFn(*blobInfo.Name); err != nil {
					return err
				}
			}
			if marker = resp.NextMarker; marker == nil {
				return nil
			}
		}
	}
}

func isErrNotFound(err error) bool {
	var responseErr *azcore.ResponseError

	return errors.As(err, &responseErr) && responseErr.ErrorCode == string(bloberror.BlobNotFound)
}

func (a *Adapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	var err error
	defer reportMetrics("Exists", time.Now(), nil, &err)

	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return false, err
	}

	blobURL := a.client.NewContainerClient(qualifiedKey.ContainerName).NewBlobClient(qualifiedKey.BlobURL)

	_, err = blobURL.GetProperties(ctx, nil)

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

	container := a.client.NewContainerClient(qualifiedKey.ContainerName)
	blobURL := container.NewBlobClient(qualifiedKey.BlobURL)

	props, err := blobURL.GetProperties(ctx, nil)
	if err != nil {
		return block.Properties{}, err
	}
	return block.Properties{StorageClass: props.AccessTier}, nil
}

func (a *Adapter) Remove(ctx context.Context, obj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Remove", time.Now(), nil, &err)
	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return err
	}

	container := a.client.NewContainerClient(qualifiedKey.ContainerName)
	blobURL := container.NewBlobClient(qualifiedKey.BlobURL)

	_, err = blobURL.Delete(ctx, nil)
	return err
}

func (a *Adapter) Copy(ctx context.Context, sourceObj, destinationObj block.ObjectPointer) error {
	// return ErrNotImplemented
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

	destinationContainer := a.client.NewContainerClient(qualifiedDestinationKey.ContainerName)
	destinationURL := destinationContainer.NewBlobClient(qualifiedDestinationKey.BlobURL)
	resp, err := destinationURL.StartCopyFromURL(ctx, qualifiedSourceKey.BlobURL, nil)
	if err != nil {
		return err
	}
	// validate copy is not asynchronous
	copyStatus := resp.CopyStatus
	if *copyStatus == blob.CopyStatusTypePending {
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

func (a *Adapter) UploadPart(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, _ string, _ int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), nil, &err)

	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return nil, err
	}

	container := a.client.NewContainerClient(qualifiedKey.ContainerName)
	hashReader := block.NewHashingReader(reader, block.HashFunctionMD5)

	multipartBlockWriter := NewMultipartBlockWriter(hashReader, *container, qualifiedKey.BlobURL)
	res, err := copyFromReader(ctx, hashReader, multipartBlockWriter, blockblob.UploadStreamOptions{
		BlockSize:   sizeBytes,
		Concurrency: 1,
	})
	if err != nil {
		return nil, err
	}
	return &block.UploadPartResponse{
		ETag: strings.Trim(string(*res.ETag), `"`),
	}, nil
}

// TODO (niro): Need to fix this
func (a *Adapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, _ string, _ int) (*block.UploadPartResponse, error) {
	var err error
	defer reportMetrics("UploadPart", time.Now(), nil, &err)

	return a.copyPartRange(ctx, sourceObj, destinationObj, 0, blockblob.CountToEnd)
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

	destinationContainer := a.client.NewContainerClient(qualifiedDestinationKey.ContainerName)
	sourceContainer := a.client.NewContainerClient(qualifiedSourceKey.ContainerName)
	sourceBlobURL := sourceContainer.NewBlockBlobClient(qualifiedSourceKey.BlobURL)

	return copyPartRange(ctx, *destinationContainer, qualifiedDestinationKey.BlobURL, *sourceBlobURL, startPosition, count)
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
	containerURL := a.client.NewContainerClient(qualifiedKey.ContainerName)
	return completeMultipart(ctx, multipartList.Part, *containerURL, qualifiedKey.BlobURL)
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
