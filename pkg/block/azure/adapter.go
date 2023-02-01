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

	lru "github.com/hnlq715/golang-lru"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/logging"
)

var ErrNotImplemented = errors.New("not implemented")

const (
	sizeSuffix = "_size"
	idSuffix   = "_id"
	_1MiB      = 1024 * 1024
	MaxBuffers = 1
	// udcCacheSize - Arbitrary number: exceeding this number means that in the expiry timeframe we requested pre-signed urls from
	// more the 30 different accounts which is highly unlikely
	udcCacheSize = 30

	URLTemplate = "https://%s.blob.core.windows.net/"
)

type Adapter struct {
	clientCache *ClientCache
	// udCredCache - User Delegation Credential cache used to reduce POST requests while creating pre-signed URLs
	udCredCache     *lru.ARCCache
	preSignedExpiry time.Duration
}

func NewAdapter(params params.Azure) (*Adapter, error) {
	logging.Default().WithField("type", "azure").Info("initialized blockstore adapter")
	preSignedExpiry := params.PreSignedExpiry
	if preSignedExpiry == 0 {
		preSignedExpiry = block.DefaultPreSignExpiryDuration
	}
	l, err := lru.NewARC(udcCacheSize)
	if err != nil {
		return nil, err
	}

	return &Adapter{
		clientCache:     NewCache(params),
		udCredCache:     l,
		preSignedExpiry: preSignedExpiry,
	}, nil
}

type BlobURLInfo struct {
	StorageAccountName string
	ContainerURL       string
	ContainerName      string
	BlobURL            string
}

type PrefixURLInfo struct {
	StorageAccountName string
	ContainerURL       string
	ContainerName      string
	Prefix             string
}

func ExtractStorageAccount(storageAccount *url.URL) (string, error) {
	// In azure the subdomain is the storage account
	const expectedHostParts = 2
	hostParts := strings.SplitN(storageAccount.Host, ".", expectedHostParts)
	if len(hostParts) != expectedHostParts {
		return "", fmt.Errorf("wrong host parts(%d): %w", len(hostParts), block.ErrInvalidNamespace)
	}

	return hostParts[0], nil
}

func ResolveBlobURLInfoFromURL(pathURL *url.URL) (BlobURLInfo, error) {
	var qk BlobURLInfo
	storageType, err := block.GetStorageType(pathURL)
	if err != nil {
		return qk, err
	}
	if storageType != block.StorageTypeAzure {
		return qk, fmt.Errorf("wrong storage type: %w", block.ErrInvalidNamespace)
	}
	// In azure the first part of the path is part of the storage namespace
	trimmedPath := strings.Trim(pathURL.Path, "/")
	pathParts := strings.Split(trimmedPath, "/")
	if len(pathParts) == 0 {
		return qk, fmt.Errorf("wrong path parts(%d): %w", len(pathParts), block.ErrInvalidNamespace)
	}

	storageAccount, err := ExtractStorageAccount(pathURL)
	if err != nil {
		return qk, err
	}

	return BlobURLInfo{
		StorageAccountName: storageAccount,
		ContainerURL:       fmt.Sprintf("%s://%s/%s", pathURL.Scheme, pathURL.Host, pathParts[0]),
		ContainerName:      pathParts[0],
		BlobURL:            strings.Join(pathParts[1:], "/"),
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
			StorageAccountName: qp.StorageAccountName,
			ContainerURL:       qp.ContainerURL,
			ContainerName:      qp.ContainerName,
			BlobURL:            qp.BlobURL + "/" + key,
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
		StorageAccountName: qualifiedPrefix.StorageAccountName,
		ContainerURL:       qualifiedPrefix.ContainerURL,
		ContainerName:      qualifiedPrefix.ContainerName,
		Prefix:             qualifiedPrefix.BlobURL,
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
	containerClient, err := a.clientCache.NewContainerClient(qualifiedKey.StorageAccountName, qualifiedKey.ContainerName)
	if err != nil {
		return err
	}
	_, err = containerClient.NewBlockBlobClient(qualifiedKey.BlobURL).UploadStream(ctx, reader, &o)
	return err
}

func (a *Adapter) Get(ctx context.Context, obj block.ObjectPointer, _ int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("Get", time.Now(), nil, &err)

	return a.Download(ctx, obj, 0, blockblob.CountToEnd)
}

func (a *Adapter) GetPreSignedURL(_ context.Context, obj block.ObjectPointer, mode block.PreSignMode) (string, error) {
	permissions := sas.BlobPermissions{Read: true}
	if mode == block.PreSignModeWrite {
		permissions = sas.BlobPermissions{
			Read:  true,
			Add:   true,
			Write: true,
		}
	}
	return a.getPreSignedURL(obj, permissions)
}

func (a *Adapter) getPreSignedURL(obj block.ObjectPointer, permissions sas.BlobPermissions) (string, error) {
	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return "", err
	}

	// Use shared credential for clients initialized with storage access key
	if qualifiedKey.StorageAccountName == a.clientCache.params.StorageAccount && a.clientCache.params.StorageAccessKey != "" {
		container, err := a.clientCache.NewContainerClient(qualifiedKey.StorageAccountName, qualifiedKey.ContainerName)
		if err != nil {
			return "", err
		}
		return container.NewBlobClient(qualifiedKey.BlobURL).GetSASURL(permissions, time.Time{}, a.newPreSignedTime())
	}

	// Otherwise assume using role based credentials and build signed URL using user delegation credentials
	currentTime := time.Now().UTC().Add(-10 * time.Second)
	urlExpiry := a.newPreSignedTime()
	// UDC expiry 2 time of pre-sign expiry
	udcExpiry := urlExpiry.Add(a.preSignedExpiry)
	info := service.KeyInfo{
		Start:  to.Ptr(currentTime.UTC().Format(sas.TimeFormat)),
		Expiry: to.Ptr(udcExpiry.Format(sas.TimeFormat)),
	}
	var udc *service.UserDelegationCredential
	// Check udcCache
	res, ok := a.udCredCache.Get(qualifiedKey.StorageAccountName)
	if !ok {
		svc, err := a.clientCache.NewServiceClient(qualifiedKey.StorageAccountName)
		if err != nil {
			return "", err
		}
		udc, err = svc.GetUserDelegationCredential(context.Background(), info, nil)
		if err != nil {
			return "", err
		}
		// UDC expires after 2 * a.preSignedExpiry but cache entry expires after a.preSignedExpiry
		a.udCredCache.AddEx(qualifiedKey.StorageAccountName, udc, a.preSignedExpiry)
	} else {
		udc = res.(*service.UserDelegationCredential)
	}

	// Create Blob Signature Values with desired permissions and sign with user delegation credential
	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    a.newPreSignedTime(),
		Permissions:   to.Ptr(permissions).String(),
		ContainerName: qualifiedKey.ContainerName,
		BlobName:      qualifiedKey.BlobURL,
	}.SignWithUserDelegation(udc)
	if err != nil {
		return "", err
	}
	u := fmt.Sprintf("%s/%s?%s", qualifiedKey.ContainerURL, qualifiedKey.BlobURL, sasQueryParams.Encode())

	return u, nil
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
	container, err := a.clientCache.NewContainerClient(qualifiedKey.StorageAccountName, qualifiedKey.ContainerName)
	if err != nil {
		return nil, err
	}
	blobURL := container.NewBlockBlobClient(qualifiedKey.BlobURL)

	downloadResponse, err := blobURL.DownloadStream(ctx, &azblob.DownloadStreamOptions{
		RangeGetContentMD5: nil,
		Range: blob.HTTPRange{
			Offset: offset,
			Count:  count,
		},
	})
	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil, block.ErrDataNotFound
	}
	if err != nil {
		a.log(ctx).WithError(err).Errorf("failed to get azure blob from container %s key %s", container, blobURL)
		return nil, err
	}
	return downloadResponse.Body, nil
}

func (a *Adapter) Walk(ctx context.Context, walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	var err error
	defer reportMetrics("Walk", time.Now(), nil, &err)

	qualifiedPrefix, err := resolveNamespacePrefix(walkOpt)
	if err != nil {
		return err
	}

	containerClient, err := a.clientCache.NewContainerClient(qualifiedPrefix.StorageAccountName, qualifiedPrefix.ContainerName)
	if err != nil {
		return err
	}

	var marker *string
	for {
		listBlob := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
			Prefix: &qualifiedPrefix.Prefix,
			Marker: marker,
		})

		for listBlob.More() {
			resp, err := listBlob.NextPage(ctx)
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

func (a *Adapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	var err error
	defer reportMetrics("Exists", time.Now(), nil, &err)

	qualifiedKey, err := resolveBlobURLInfo(obj)
	if err != nil {
		return false, err
	}

	containerClient, err := a.clientCache.NewContainerClient(qualifiedKey.StorageAccountName, qualifiedKey.ContainerName)
	if err != nil {
		return false, err
	}
	blobURL := containerClient.NewBlobClient(qualifiedKey.BlobURL)

	_, err = blobURL.GetProperties(ctx, nil)

	if bloberror.HasCode(err, bloberror.BlobNotFound) {
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

	containerClient, err := a.clientCache.NewContainerClient(qualifiedKey.StorageAccountName, qualifiedKey.ContainerName)
	if err != nil {
		return block.Properties{}, err
	}
	blobURL := containerClient.NewBlobClient(qualifiedKey.BlobURL)

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
	containerClient, err := a.clientCache.NewContainerClient(qualifiedKey.StorageAccountName, qualifiedKey.ContainerName)
	if err != nil {
		return err
	}
	blobURL := containerClient.NewBlobClient(qualifiedKey.BlobURL)

	_, err = blobURL.Delete(ctx, nil)
	return err
}

func (a *Adapter) Copy(ctx context.Context, sourceObj, destinationObj block.ObjectPointer) error {
	var err error
	defer reportMetrics("Copy", time.Now(), nil, &err)

	qualifiedDestinationKey, err := resolveBlobURLInfo(destinationObj)
	if err != nil {
		return err
	}

	destContainerClient, err := a.clientCache.NewContainerClient(qualifiedDestinationKey.StorageAccountName, qualifiedDestinationKey.ContainerName)
	if err != nil {
		return err
	}
	destClient := destContainerClient.NewBlobClient(qualifiedDestinationKey.BlobURL)

	sasKey, err := a.GetPreSignedURL(ctx, sourceObj, block.PreSignModeRead)
	if err != nil {
		return err
	}

	// Optimistic flow - try to copy synchronously
	_, err = destClient.CopyFromURL(ctx, sasKey, nil)
	if err == nil {
		return nil
	}
	// Azure API (backend) returns ambiguous error code which requires us to parse the error message to understand what is the nature of the error
	// See: https://github.com/Azure/azure-sdk-for-go/issues/19880
	if !bloberror.HasCode(err, bloberror.CannotVerifyCopySource) ||
		!strings.Contains(err.Error(), "The source request body for synchronous copy is too large and exceeds the maximum permissible limit") {
		return err
	}

	// Blob too big for synchronous copy. Perform async copy
	logger := a.log(ctx).WithFields(logging.Fields{
		"sourceObj": sourceObj.Identifier,
		"destObj":   destinationObj.Identifier,
	})
	logger.Debug("Perform async copy")
	res, err := destClient.StartCopyFromURL(ctx, sasKey, nil)
	if err != nil {
		return err
	}
	copyStatus := res.CopyStatus
	if copyStatus == nil {
		return fmt.Errorf("%w: failed to get copy status", block.ErrAsyncCopy)
	}

	progress := ""
	const asyncPollInterval = 5 * time.Second
	for {
		select {
		case <-ctx.Done():
			logger.WithField("copy_progress", progress).Warn("context canceled, aborting copy")
			// Context canceled - perform abort on copy use a different context for the abort
			_, err := destClient.AbortCopyFromURL(context.Background(), *res.CopyID, nil)
			if err != nil {
				logger.WithError(err).Error("failed to abort copy")
			}
			return ctx.Err()

		case <-time.After(asyncPollInterval):
			p, err := destClient.GetProperties(ctx, nil)
			if err != nil {
				return err
			}
			copyStatus = p.CopyStatus
			if copyStatus == nil {
				return fmt.Errorf("%w: failed to get copy status", block.ErrAsyncCopy)
			}
			progress = *p.CopyProgress
			switch *copyStatus {
			case blob.CopyStatusTypeSuccess:
				logger.WithField("object_properties", p).Debug("Async copy successful")
				return nil

			case blob.CopyStatusTypeAborted:
				return fmt.Errorf("%w: unexpected abort", block.ErrAsyncCopy)

			case blob.CopyStatusTypeFailed:
				return fmt.Errorf("%w: copy status failed", block.ErrAsyncCopy)

			case blob.CopyStatusTypePending:
				logger.WithField("copy_progress", progress).Debug("Copy pending")

			default:
				return fmt.Errorf("%w: invalid copy status: %s", block.ErrAsyncCopy, *copyStatus)
			}
		}
	}
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

	container, err := a.clientCache.NewContainerClient(qualifiedKey.StorageAccountName, qualifiedKey.ContainerName)
	if err != nil {
		return nil, err
	}
	hashReader := block.NewHashingReader(reader, block.HashFunctionMD5)

	multipartBlockWriter := NewMultipartBlockWriter(hashReader, *container, qualifiedKey.BlobURL)
	_, err = copyFromReader(ctx, hashReader, multipartBlockWriter, blockblob.UploadStreamOptions{
		BlockSize:   _1MiB,
		Concurrency: MaxBuffers,
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

	destinationContainer, err := a.clientCache.NewContainerClient(qualifiedDestinationKey.StorageAccountName, qualifiedDestinationKey.ContainerName)
	if err != nil {
		return nil, err
	}
	sourceContainer, err := a.clientCache.NewContainerClient(qualifiedSourceKey.StorageAccountName, qualifiedSourceKey.ContainerName)
	if err != nil {
		return nil, err
	}

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
	containerURL, err := a.clientCache.NewContainerClient(qualifiedKey.StorageAccountName, qualifiedKey.ContainerName)
	if err != nil {
		return nil, err
	}

	return completeMultipart(ctx, multipartList.Part, *containerURL, qualifiedKey.BlobURL)
}

func (a *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	return block.StorageNamespaceInfo{
		ValidityRegex:  `^https?://`,
		Example:        "https://mystorageaccount.blob.core.windows.net/mycontainer/",
		PreSignSupport: true,
	}
}

func (a *Adapter) RuntimeStats() map[string]string {
	return nil
}

func (a *Adapter) newPreSignedTime() time.Time {
	return time.Now().UTC().Add(a.preSignedExpiry)
}
