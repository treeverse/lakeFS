package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	sizeSuffix = "_size"
	idSuffix   = "_id"
	_1MiB      = 1024 * 1024
	MaxBuffers = 1
	// udcCacheSize - Arbitrary number: exceeding this number means that in the expiry timeframe we requested pre-signed urls from
	// more the 5000 different accounts, which is highly unlikely
	udcCacheSize = 5000

	BlobEndpointDefaultDomain = "blob.core.windows.net"
	BlobEndpointChinaDomain   = "blob.core.chinacloudapi.cn"
	BlobEndpointUSGovDomain   = "blob.core.usgovcloudapi.net"
	BlobEndpointTestDomain    = "azurite.test"
)

var (
	ErrInvalidDomain = errors.New("invalid Azure Domain")

	endpointRegex    = regexp.MustCompile(`https://(?P<account>[\w]+).(?P<domain>[\w.-]+)[/:][\w-/,]*$`)
	supportedDomains = []string{
		BlobEndpointDefaultDomain,
		BlobEndpointChinaDomain,
		BlobEndpointUSGovDomain,
		BlobEndpointTestDomain,
	}
)

type Adapter struct {
	clientCache        *ClientCache
	preSignedExpiry    time.Duration
	disablePreSigned   bool
	disablePreSignedUI bool
}

func NewAdapter(ctx context.Context, params params.Azure) (*Adapter, error) {
	logging.FromContext(ctx).WithField("type", "azure").Info("initialized blockstore adapter")
	preSignedExpiry := params.PreSignedExpiry
	if preSignedExpiry == 0 {
		preSignedExpiry = block.DefaultPreSignExpiryDuration
	}

	if params.Domain == "" {
		params.Domain = BlobEndpointDefaultDomain
	} else if !slices.Contains(supportedDomains, params.Domain) {
		return nil, ErrInvalidDomain
	}

	cache, err := NewCache(params)
	if err != nil {
		return nil, err
	}

	return &Adapter{
		clientCache:        cache,
		preSignedExpiry:    preSignedExpiry,
		disablePreSigned:   params.DisablePreSigned,
		disablePreSignedUI: params.DisablePreSignedUI,
	}, nil
}

type BlobURLInfo struct {
	StorageAccountName string
	ContainerURL       string
	ContainerName      string
	BlobURL            string
	Host               string
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
		return "", fmt.Errorf("wrong host parts(%d): %w", len(hostParts), block.ErrInvalidAddress)
	}

	return hostParts[0], nil
}

func ResolveBlobURLInfoFromURL(pathURL *url.URL) (BlobURLInfo, error) {
	var qk BlobURLInfo
	err := block.ValidateStorageType(pathURL, block.StorageTypeAzure)
	if err != nil {
		return qk, err
	}

	// In azure, the first part of the path is part of the storage namespace
	trimmedPath := strings.Trim(pathURL.Path, "/")
	pathParts := strings.Split(trimmedPath, "/")
	if len(pathParts) == 0 {
		return qk, fmt.Errorf("wrong path parts(%d): %w", len(pathParts), block.ErrInvalidAddress)
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
		Host:               pathURL.Host,
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
			Host:               parsedNamespace.Host,
		}
		if qp.BlobURL == "" {
			info.BlobURL = key
		}
		return info, nil
	}
	return ResolveBlobURLInfoFromURL(parsedKey)
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

func (a *Adapter) GetWalker(uri *url.URL) (block.Walker, error) {
	if err := block.ValidateStorageType(uri, block.StorageTypeAzure); err != nil {
		return nil, err
	}

	storageAccount, domain, err := ParseURL(uri)
	if err != nil {
		return nil, err
	}
	if domain != a.clientCache.params.Domain {
		return nil, fmt.Errorf("domain mismatch! expected: %s, got: %s. %w", a.clientCache.params.Domain, domain, ErrInvalidDomain)
	}

	client, err := a.clientCache.NewServiceClient(storageAccount)
	if err != nil {
		return nil, err
	}

	return NewAzureDataLakeWalker(client, false)
}

func (a *Adapter) GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode) (string, time.Time, error) {
	if a.disablePreSigned {
		return "", time.Time{}, block.ErrOperationNotSupported
	}

	permissions := sas.BlobPermissions{Read: true}
	if mode == block.PreSignModeWrite {
		permissions = sas.BlobPermissions{
			Read:  true,
			Add:   true,
			Write: true,
		}
	}
	preSignedURL, err := a.getPreSignedURL(ctx, obj, permissions)
	// TODO(#6347): Report expiry.
	return preSignedURL, time.Time{}, err
}

func (a *Adapter) getPreSignedURL(ctx context.Context, obj block.ObjectPointer, permissions sas.BlobPermissions) (string, error) {
	if a.disablePreSigned {
		return "", block.ErrOperationNotSupported
	}

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
		client := container.NewBlobClient(qualifiedKey.BlobURL)
		urlExpiry := a.newPreSignedTime()
		return client.GetSASURL(permissions, urlExpiry, &blob.GetSASURLOptions{})
	}

	// Otherwise assume using role based credentials and build signed URL using user delegation credentials
	urlExpiry := a.newPreSignedTime()
	udc, err := a.clientCache.NewUDC(ctx, qualifiedKey.StorageAccountName, &urlExpiry)
	if err != nil {
		return "", err
	}

	// Create Blob Signature Values with desired permissions and sign with user delegation credential
	blobSignatureValues := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		ExpiryTime:    urlExpiry,
		Permissions:   to.Ptr(permissions).String(),
		ContainerName: qualifiedKey.ContainerName,
		BlobName:      qualifiedKey.BlobURL,
	}
	sasQueryParams, err := blobSignatureValues.SignWithUserDelegation(udc)
	if err != nil {
		return "", err
	}

	var accountEndpoint string
	// format blob URL with signed SAS query params
	if a.clientCache.params.TestEndpointURL != "" {
		accountEndpoint = a.clientCache.params.TestEndpointURL
	} else {
		accountEndpoint = buildAccountEndpoint(qualifiedKey.StorageAccountName, a.clientCache.params.Domain)
	}

	u, err := url.JoinPath(accountEndpoint, qualifiedKey.ContainerName, qualifiedKey.BlobURL)
	if err != nil {
		return "", err
	}
	u += "?" + sasQueryParams.Encode()
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

	sasKey, _, err := a.GetPreSignedURL(ctx, sourceObj, block.PreSignModeRead)
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
		return fmt.Errorf("%w: failed to get copy status", block.ErrAsyncCopyFailed)
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
				return fmt.Errorf("%w: failed to get copy status", block.ErrAsyncCopyFailed)
			}
			progress = *p.CopyProgress
			switch *copyStatus {
			case blob.CopyStatusTypeSuccess:
				logger.WithField("object_properties", p).Debug("Async copy successful")
				return nil

			case blob.CopyStatusTypeAborted:
				return fmt.Errorf("%w: unexpected abort", block.ErrAsyncCopyFailed)

			case blob.CopyStatusTypeFailed:
				return fmt.Errorf("%w: copy status failed", block.ErrAsyncCopyFailed)

			case blob.CopyStatusTypePending:
				logger.WithField("copy_progress", progress).Debug("Copy pending")

			default:
				return fmt.Errorf("%w: invalid copy status: %s", block.ErrAsyncCopyFailed, *copyStatus)
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

	return copyPartRange(ctx, a.clientCache, qualifiedDestinationKey, qualifiedSourceKey, startPosition, count)
}

func (a *Adapter) AbortMultiPartUpload(_ context.Context, _ block.ObjectPointer, _ string) error {
	// Azure has no abort. In case of commit, uncommitted parts are erased. Otherwise, staged data is erased after 7 days
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
	info := block.DefaultStorageNamespaceInfo(block.BlockstoreTypeAzure)

	info.ImportValidityRegex = fmt.Sprintf(`^https?://[a-z0-9_-]+\.%s`, a.clientCache.params.Domain)
	info.ValidityRegex = fmt.Sprintf(`^https?://[a-z0-9_-]+\.%s`, a.clientCache.params.Domain)

	info.Example = fmt.Sprintf("https://mystorageaccount.%s/mycontainer/", a.clientCache.params.Domain)
	if a.disablePreSigned {
		info.PreSignSupport = false
	}
	if !(a.disablePreSignedUI || a.disablePreSigned) {
		info.PreSignSupportUI = true
	}
	return info
}

func (a *Adapter) ResolveNamespace(storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	return block.DefaultResolveNamespace(storageNamespace, key, identifierType)
}

func (a *Adapter) RuntimeStats() map[string]string {
	return nil
}

func (a *Adapter) newPreSignedTime() time.Time {
	return time.Now().UTC().Add(a.preSignedExpiry)
}

func (a *Adapter) GetPresignUploadPartURL(_ context.Context, _ block.ObjectPointer, _ string, _ int) (string, error) {
	return "", block.ErrOperationNotSupported
}

func (a *Adapter) ListParts(_ context.Context, _ block.ObjectPointer, _ string, _ block.ListPartsOpts) (*block.ListPartsResponse, error) {
	return nil, block.ErrOperationNotSupported
}

// ParseURL - parses url and extracts account name and domain. If either are not found returns an error
func ParseURL(uri *url.URL) (accountName string, domain string, err error) {
	u, err := uri.Parse("")
	if err != nil {
		return "", "", err
	}

	u.RawQuery = ""
	matches := endpointRegex.FindStringSubmatch(u.String())
	if matches == nil {
		return "", "", ErrAzureInvalidURL
	}

	domainIdx := endpointRegex.SubexpIndex("domain")
	if domainIdx < 0 {
		return "", "", fmt.Errorf("invalid domain: %w", ErrInvalidDomain)
	}

	accountIdx := endpointRegex.SubexpIndex("account")
	if accountIdx < 0 {
		return "", "", fmt.Errorf("missing storage account: %w", ErrAzureInvalidURL)
	}

	return matches[accountIdx], matches[domainIdx], nil
}
