package azure

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	guuid "github.com/google/uuid"

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
	defaultMaxRetryRequests = 0
)

type Adapter struct {
	ctx            context.Context
	p              pipeline.Pipeline
	accountName    string
	configurations configurations
}

type configurations struct {
	retryReaderOptions azblob.RetryReaderOptions
}

func NewAdapter(pipeline pipeline.Pipeline, accountName string, opts ...func(a *Adapter)) *Adapter {
	a := &Adapter{
		ctx:            context.Background(),
		accountName:    accountName,
		p:              pipeline,
		configurations: configurations{retryReaderOptions: azblob.RetryReaderOptions{MaxRetryRequests: defaultMaxRetryRequests}},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		p:           a.p,
		accountName: a.accountName,
		ctx:         ctx,
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
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", a.accountName, containerName))

	return azblob.NewContainerURL(*URL, a.p)
}

func (a *Adapter) getBlobURL(containerName, fileName string) azblob.BlobURL {
	containerURL := a.getContainerURL(containerName)

	return containerURL.NewBlobURL(fileName)
}

func (a *Adapter) getBlockBlobURL(containerName, fileName string) azblob.BlockBlobURL {
	containerURL := a.getContainerURL(containerName)

	return containerURL.NewBlockBlobURL(fileName)
}

func (a *Adapter) getIDURL(containerName, fileName string) azblob.BlockBlobURL {
	return a.getBlockBlobURL(containerName, fileName+idSuffix)
}

func (a *Adapter) getSizeURL(containerName, fileName string) azblob.BlockBlobURL {
	return a.getBlockBlobURL(containerName, fileName+sizeSuffix)
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
	blobURL := a.getBlockBlobURL(qualifiedKey.StorageNamespace, qualifiedKey.Key)

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
	blobURL := a.getBlobURL(qualifiedKey.StorageNamespace, qualifiedKey.Key)

	keyOptions := azblob.ClientProvidedKeyOptions{}
	downloadResponse, err := blobURL.Download(a.ctx, offset, count, azblob.BlobAccessConditions{}, false, keyOptions)

	if err != nil {
		return nil, err
	}
	// NOTE: automatically retries are performed if the connection fails
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

	blobURL := a.getBlobURL(qualifiedKey.StorageNamespace, qualifiedKey.Key)
	_, err = blobURL.GetProperties(a.ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		if err.(azblob.StorageError).ServiceCode() == azblob.ServiceCodeBlobNotFound {
			return false, nil
		}
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

	blobURL := a.getBlobURL(qualifiedKey.StorageNamespace, qualifiedKey.Key)
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

	containerName := qualifiedKey.StorageNamespace
	fileName := qualifiedKey.Key
	blobURL := a.getBlobURL(containerName, fileName)
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

	sourceURL := a.getBlobURL(qualifiedSourceKey.StorageNamespace, qualifiedSourceKey.Key)
	destinationURL := a.getBlobURL(qualifiedDestinationKey.StorageNamespace, qualifiedDestinationKey.Key)
	_, err = destinationURL.StartCopyFromURL(a.ctx, sourceURL.URL(), azblob.Metadata{}, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{}, azblob.AccessTierNone, azblob.BlobTagsMap{})
	return err
}

func (a *Adapter) CreateMultiPartUpload(obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (string, error) {
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

	containerName := qualifiedKey.StorageNamespace
	objName := qualifiedKey.Key

	blobURL := a.getBlockBlobURL(containerName, objName)
	blobIDsURL := a.getIDURL(containerName, objName)
	blobSizesURL := a.getSizeURL(containerName, objName)

	hashReader := block.NewHashingReader(reader, block.HashFunctionMD5)

	multipartBlockWriter := NewMultipartBlockWriter(hashReader, blobURL, blobIDsURL, blobSizesURL)
	_, err = copyFromReader(a.ctx, hashReader, multipartBlockWriter, azblob.UploadStreamToBlockBlobOptions{})
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

func generateRandomBlockID() string {
	uu := guuid.New()
	u := [64]byte{}
	copy(u[:], uu[:])
	return base64.StdEncoding.EncodeToString(u[:])
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

	containerName := qualifiedDestinationKey.StorageNamespace
	objName := qualifiedDestinationKey.Key

	blobURL := a.getBlockBlobURL(containerName, objName)

	sourceBlobURL := a.getBlockBlobURL(qualifiedSourceKey.StorageNamespace, qualifiedSourceKey.Key)

	base64BlockID := generateRandomBlockID()
	_, err = blobURL.StageBlockFromURL(a.ctx, base64BlockID, sourceBlobURL.URL(), startPosition, count, azblob.LeaseAccessConditions{}, azblob.ModifiedAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", err
	}

	// add size and id to etag
	response, err := sourceBlobURL.GetProperties(a.ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", err
	}
	etag := "\"" + hex.EncodeToString(response.ContentMD5()) + "\""
	size := response.ContentLength()
	base64Etag := base64.StdEncoding.EncodeToString([]byte(etag))
	// stage id data
	blobIDsURL := a.getIDURL(containerName, objName)
	_, err = blobIDsURL.StageBlock(a.ctx, base64Etag, strings.NewReader(base64BlockID+"\n"), azblob.LeaseAccessConditions{}, nil, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", fmt.Errorf("failed staging part data: %w", err)
	}

	// stage size data
	sizeData := strconv.Itoa(int(size)) + "\n"
	blobSizesURL := a.getSizeURL(containerName, objName)
	_, err = blobSizesURL.StageBlock(a.ctx, base64Etag, strings.NewReader(sizeData), azblob.LeaseAccessConditions{}, nil, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", fmt.Errorf("failed staging part data: %w", err)
	}

	return etag, nil
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
	containerName := qualifiedKey.StorageNamespace
	_ = a.log().WithFields(logging.Fields{
		"upload_id":     uploadID,
		"qualified_ns":  qualifiedKey.StorageNamespace,
		"qualified_key": qualifiedKey.Key,
		"key":           obj.Identifier,
	})

	parts := multipartList.Part
	sort.Slice(parts, func(i, j int) bool {
		return *parts[i].PartNumber < *parts[j].PartNumber
	})
	// extract staging blockIDs
	metaBlockIDs := make([]string, 0)
	for _, part := range multipartList.Part {
		base64Etag := base64.StdEncoding.EncodeToString([]byte(*part.ETag))
		metaBlockIDs = append(metaBlockIDs, base64Etag)
	}

	fileName := qualifiedKey.Key
	stageBlockIDs, err := a.getMultipartIDs(containerName, fileName, metaBlockIDs)

	size, err := a.getMultipartSize(containerName, fileName, metaBlockIDs)

	blobURL := a.getBlockBlobURL(containerName, fileName)

	res, err := blobURL.CommitBlockList(a.ctx, stageBlockIDs, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.AccessTierNone, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, 0, err
	}
	etag := string(res.ETag())
	etag = etag[1 : len(etag)-2]
	// list bucket parts and validate request match
	return &etag, int64(size), nil
}

func (a *Adapter) getMultipartIDs(containerName, fileName string, base64BlockIDs []string) ([]string, error) {
	blobURL := a.getIDURL(containerName, fileName)
	_, err := blobURL.CommitBlockList(a.ctx, base64BlockIDs, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.AccessTierNone, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}

	downloadResponse, err := blobURL.Download(a.ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	bodyStream := downloadResponse.Body(a.configurations.retryReaderOptions)
	scanner := bufio.NewScanner(bodyStream)
	ids := make([]string, 0)
	for scanner.Scan() {
		id := scanner.Text()
		ids = append(ids, id)
	}

	// remove
	_, err = blobURL.Delete(a.ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		a.log().WithError(err).Warn("Failed to delete multipart ids data file")
	}
	return ids, nil
}

func (a *Adapter) getMultipartSize(containerName, fileName string, base64BlockIDs []string) (int, error) {
	sizeURL := a.getSizeURL(containerName, fileName)
	_, err := sizeURL.CommitBlockList(a.ctx, base64BlockIDs, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{}, "", azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return 0, err
	}

	downloadResponse, err := sizeURL.Download(a.ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return 0, err
	}
	bodyStream := downloadResponse.Body(a.configurations.retryReaderOptions)
	scanner := bufio.NewScanner(bodyStream)
	size := 0
	for scanner.Scan() {
		s := scanner.Text()
		stageSize, err := strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
		size += stageSize
	}

	// remove
	_, err = sizeURL.Delete(a.ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		a.log().WithError(err).Warn("Failed to delete multipart size data file")
	}
	return size, nil
}
