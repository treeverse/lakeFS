package azure

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

type MultipartBlockWriter struct {
	reader *block.HashingReader // the reader that would be passed to copyFromReader, this is needed in order to get size and md5
	// to is the location we are writing our chunks to.
	to      azblob.BlockBlobURL
	toIDs   azblob.BlockBlobURL
	toSizes azblob.BlockBlobURL
	etag    string
}

func NewMultipartBlockWriter(reader *block.HashingReader, containerURL azblob.ContainerURL, objName string) *MultipartBlockWriter {
	return &MultipartBlockWriter{
		reader:  reader,
		to:      containerURL.NewBlockBlobURL(objName),
		toIDs:   containerURL.NewBlockBlobURL(objName + idSuffix),
		toSizes: containerURL.NewBlockBlobURL(objName + sizeSuffix),
	}
}

func (m *MultipartBlockWriter) StageBlock(ctx context.Context, s string, seeker io.ReadSeeker, conditions azblob.LeaseAccessConditions, bytes []byte, options azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobStageBlockResponse, error) {
	return m.to.StageBlock(ctx, s, seeker, conditions, bytes, options)
}

func (m *MultipartBlockWriter) CommitBlockList(ctx context.Context, ids []string, headers azblob.BlobHTTPHeaders, metadata azblob.Metadata, conditions azblob.BlobAccessConditions, tierType azblob.AccessTierType, tagsMap azblob.BlobTagsMap, options azblob.ClientProvidedKeyOptions) (*azblob.BlockBlobCommitBlockListResponse, error) {
	m.etag = "\"" + hex.EncodeToString(m.reader.Md5.Sum(nil)) + "\""
	base64Etag := base64.StdEncoding.EncodeToString([]byte(m.etag))

	// write to blockIDs
	pd := strings.Join(ids, "\n") + "\n"
	_, err := m.toIDs.StageBlock(ctx, base64Etag, strings.NewReader(pd), conditions.LeaseAccessConditions, nil, options)
	if err != nil {
		return nil, fmt.Errorf("failed staging part data: %w", err)
	}
	// write block sizes
	sd := strconv.Itoa(int(m.reader.CopiedSize)) + "\n"
	_, err = m.toSizes.StageBlock(ctx, base64Etag, strings.NewReader(sd), conditions.LeaseAccessConditions, nil, options)
	if err != nil {
		return nil, fmt.Errorf("failed staging part data: %w", err)
	}

	return &azblob.BlockBlobCommitBlockListResponse{}, err
}

func CompleteMultipart(ctx context.Context, parts []*s3.CompletedPart, container azblob.ContainerURL, objName string, retryOptions azblob.RetryReaderOptions) (*string, int64, error) {
	sort.Slice(parts, func(i, j int) bool {
		return *parts[i].PartNumber < *parts[j].PartNumber
	})
	// extract staging blockIDs
	metaBlockIDs := make([]string, len(parts))
	for i, part := range parts {
		base64Etag := base64.StdEncoding.EncodeToString([]byte(*part.ETag))
		metaBlockIDs[i] = base64Etag
	}

	stageBlockIDs, err := getMultipartIDs(ctx, container, objName, metaBlockIDs, retryOptions)
	if err != nil {
		return nil, 0, err
	}
	size, err := getMultipartSize(ctx, container, objName, metaBlockIDs, retryOptions)
	if err != nil {
		return nil, 0, err
	}
	blobURL := container.NewBlockBlobURL(objName)

	res, err := blobURL.CommitBlockList(ctx, stageBlockIDs, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.AccessTierNone, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, 0, err
	}
	etag := string(res.ETag())
	return &etag, int64(size), nil
}

func getMultipartIDs(ctx context.Context, container azblob.ContainerURL, objName string, base64BlockIDs []string, retryOptions azblob.RetryReaderOptions) ([]string, error) {
	blobURL := container.NewBlockBlobURL(objName + idSuffix)
	_, err := blobURL.CommitBlockList(ctx, base64BlockIDs, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{}, azblob.AccessTierNone, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}

	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	bodyStream := downloadResponse.Body(retryOptions)
	defer func() {
		_ = bodyStream.Close()
	}()
	scanner := bufio.NewScanner(bodyStream)
	ids := make([]string, 0)
	for scanner.Scan() {
		id := scanner.Text()
		ids = append(ids, id)
	}

	// remove
	_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		logging.Default().WithContext(ctx).WithField("blob_url", blobURL.String()).WithError(err).Warn("Failed to delete multipart ids data file")
	}
	return ids, nil
}

func getMultipartSize(ctx context.Context, container azblob.ContainerURL, objName string, base64BlockIDs []string, retryOptions azblob.RetryReaderOptions) (int, error) {
	blobURL := container.NewBlockBlobURL(objName + sizeSuffix)
	_, err := blobURL.CommitBlockList(ctx, base64BlockIDs, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{}, "", azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return 0, err
	}

	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return 0, err
	}
	bodyStream := downloadResponse.Body(retryOptions)
	defer func() {
		_ = bodyStream.Close()
	}()
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
	_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		logging.Default().WithContext(ctx).WithField("blob_url", blobURL.String()).WithError(err).Warn("Failed to delete multipart size data file")
	}
	return size, nil
}

func copyPartRange(ctx context.Context, destinationContainer azblob.ContainerURL, destinationObjName string, sourceBlobURL azblob.BlockBlobURL, startPosition, count int64) (string, error) {
	base64BlockID := generateRandomBlockID()
	_, err := sourceBlobURL.StageBlockFromURL(ctx, base64BlockID, sourceBlobURL.URL(), startPosition, count, azblob.LeaseAccessConditions{}, azblob.ModifiedAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", err
	}

	// add size and id to etag
	response, err := sourceBlobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", err
	}
	etag := "\"" + hex.EncodeToString(response.ContentMD5()) + "\""
	size := response.ContentLength()
	base64Etag := base64.StdEncoding.EncodeToString([]byte(etag))
	// stage id data
	blobIDsURL := destinationContainer.NewBlockBlobURL(destinationObjName + idSuffix)
	_, err = blobIDsURL.StageBlock(ctx, base64Etag, strings.NewReader(base64BlockID+"\n"), azblob.LeaseAccessConditions{}, nil, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", fmt.Errorf("failed staging part data: %w", err)
	}

	// stage size data
	sizeData := strconv.Itoa(int(size)) + "\n"
	blobSizesURL := destinationContainer.NewBlockBlobURL(destinationObjName + sizeSuffix)
	_, err = blobSizesURL.StageBlock(ctx, base64Etag, strings.NewReader(sizeData), azblob.LeaseAccessConditions{}, nil, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return "", fmt.Errorf("failed staging part data: %w", err)
	}

	return etag, nil
}

func generateRandomBlockID() string {
	uu := uuid.New()
	u := [64]byte{}
	copy(u[:], uu[:])
	return base64.StdEncoding.EncodeToString(u[:])
}
