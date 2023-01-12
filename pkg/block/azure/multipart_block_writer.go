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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

type MultipartBlockWriter struct {
	reader *block.HashingReader // the reader that would be passed to copyFromReader, this is needed in order to get size and md5
	// to is the location we are writing our chunks to.
	to      *blockblob.Client
	toIDs   *blockblob.Client
	toSizes *blockblob.Client
	etag    string
}

func NewMultipartBlockWriter(reader *block.HashingReader, containerURL container.Client, objName string) *MultipartBlockWriter {
	return &MultipartBlockWriter{
		reader:  reader,
		to:      containerURL.NewBlockBlobClient(objName),
		toIDs:   containerURL.NewBlockBlobClient(objName + idSuffix),
		toSizes: containerURL.NewBlockBlobClient(objName + sizeSuffix),
	}
}

func (m *MultipartBlockWriter) StageBlock(ctx context.Context, base64BlockID string, body io.ReadSeekCloser, options *blockblob.StageBlockOptions) (blockblob.StageBlockResponse, error) {
	return m.to.StageBlock(ctx, base64BlockID, body, options)
}

func (m *MultipartBlockWriter) CommitBlockList(ctx context.Context, ids []string, options *blockblob.CommitBlockListOptions) (blockblob.CommitBlockListResponse, error) {
	m.etag = "\"" + hex.EncodeToString(m.reader.Md5.Sum(nil)) + "\""
	base64Etag := base64.StdEncoding.EncodeToString([]byte(m.etag))

	// write to blockIDs
	pd := strings.Join(ids, "\n") + "\n"
	var leaseAccessConditions *blob.LeaseAccessConditions
	if options.AccessConditions != nil {
		leaseAccessConditions = options.AccessConditions.LeaseAccessConditions
	}
	_, err := m.toIDs.StageBlock(ctx, base64Etag, streaming.NopCloser(strings.NewReader(pd)), &blockblob.StageBlockOptions{
		LeaseAccessConditions: leaseAccessConditions,
	})
	if err != nil {
		return blockblob.CommitBlockListResponse{}, fmt.Errorf("failed staging part data: %w", err)
	}
	// write block sizes
	sd := strconv.Itoa(int(m.reader.CopiedSize)) + "\n"
	_, err = m.toSizes.StageBlock(ctx, base64Etag, streaming.NopCloser(strings.NewReader(sd)), &blockblob.StageBlockOptions{
		LeaseAccessConditions: leaseAccessConditions,
	})
	if err != nil {
		return blockblob.CommitBlockListResponse{}, fmt.Errorf("failed staging part data: %w", err)
	}

	return blockblob.CommitBlockListResponse{}, err
}

func (m *MultipartBlockWriter) Upload(_ context.Context, _ io.ReadSeekCloser, _ *blockblob.UploadOptions) (blockblob.UploadResponse, error) {
	panic("Should not be called")
}

func completeMultipart(ctx context.Context, parts []block.MultipartPart, container container.Client, objName string) (*block.CompleteMultiPartUploadResponse, error) {
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	// extract staging blockIDs
	metaBlockIDs := make([]string, len(parts))
	for i, part := range parts {
		// add Quotations marks (") if missing, Etags sent by spark include Quotations marks, Etags sent aws cli don't include Quotations marks
		etag := strings.Trim(part.ETag, "\"")
		etag = "\"" + etag + "\""
		base64Etag := base64.StdEncoding.EncodeToString([]byte(etag))
		metaBlockIDs[i] = base64Etag
	}

	stageBlockIDs, err := getMultipartIDs(ctx, container, objName, metaBlockIDs)
	if err != nil {
		return nil, err
	}

	size, err := getMultipartSize(ctx, container, objName, metaBlockIDs)
	if err != nil {
		return nil, err
	}
	blobURL := container.NewBlockBlobClient(objName)

	res, err := blobURL.CommitBlockList(ctx, stageBlockIDs, nil)
	if err != nil {
		return nil, err
	}
	etag := string(*res.ETag)
	return &block.CompleteMultiPartUploadResponse{
		ETag:          etag,
		ContentLength: size,
	}, nil
}

func getMultipartIDs(ctx context.Context, container container.Client, objName string, base64BlockIDs []string) ([]string, error) {
	blobURL := container.NewBlockBlobClient(objName + idSuffix)
	_, err := blobURL.CommitBlockList(ctx, base64BlockIDs, nil)
	if err != nil {
		return nil, err
	}

	downloadResponse, err := blobURL.DownloadStream(ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: 0,
			Count:  blockblob.CountToEnd,
		},
	})
	if err != nil {
		return nil, err
	}
	bodyStream := downloadResponse.Body
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
	_, err = blobURL.Delete(ctx, nil)
	if err != nil {
		logging.Default().WithContext(ctx).WithField("blob_url", blobURL.URL()).WithError(err).Warn("Failed to delete multipart ids data file")
	}
	return ids, nil
}

func getMultipartSize(ctx context.Context, container container.Client, objName string, base64BlockIDs []string) (int64, error) {
	blobURL := container.NewBlockBlobClient(objName + sizeSuffix)
	_, err := blobURL.CommitBlockList(ctx, base64BlockIDs, nil)
	if err != nil {
		return 0, err
	}

	downloadResponse, err := blobURL.DownloadStream(ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: 0,
			Count:  blockblob.CountToEnd,
		},
	})
	if err != nil {
		return 0, err
	}
	bodyStream := downloadResponse.Body
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
	_, err = blobURL.Delete(ctx, nil)
	if err != nil {
		logging.Default().WithContext(ctx).WithField("blob_url", blobURL.URL()).WithError(err).Warn("Failed to delete multipart size data file")
	}
	return int64(size), nil
}

func copyPartRange(ctx context.Context, destinationContainer container.Client, destinationObjName string, sourceBlobURL blockblob.Client, startPosition, count int64) (*block.UploadPartResponse, error) {
	base64BlockID := generateRandomBlockID()
	_, err := sourceBlobURL.StageBlockFromURL(ctx, base64BlockID, sourceBlobURL.URL(), count, &blockblob.StageBlockFromURLOptions{
		Range: blob.HTTPRange{
			Offset: startPosition,
			Count:  count,
		},
	})
	if err != nil {
		return nil, err
	}

	// add size and id to etag
	response, err := sourceBlobURL.GetProperties(ctx, nil)
	if err != nil {
		return nil, err
	}
	etag := "\"" + hex.EncodeToString(response.ContentMD5) + "\""
	size := response.ContentLength
	base64Etag := base64.StdEncoding.EncodeToString([]byte(etag))
	// stage id data
	blobIDsURL := destinationContainer.NewBlockBlobClient(destinationObjName + idSuffix)
	_, err = blobIDsURL.StageBlock(ctx, base64Etag, streaming.NopCloser(strings.NewReader(base64BlockID+"\n")), nil)
	if err != nil {
		return nil, fmt.Errorf("failed staging part data: %w", err)
	}

	// stage size data
	sizeData := fmt.Sprintf("%d\n", size)
	blobSizesURL := destinationContainer.NewBlockBlobClient(destinationObjName + sizeSuffix)
	_, err = blobSizesURL.StageBlock(ctx, base64Etag, streaming.NopCloser(strings.NewReader(sizeData)), nil)
	if err != nil {
		return nil, fmt.Errorf("failed staging part data: %w", err)
	}

	return &block.UploadPartResponse{
		ETag: strings.Trim(etag, `"`),
	}, nil
}

func generateRandomBlockID() string {
	uu := uuid.New()
	u := [64]byte{}
	copy(u[:], uu[:])
	return base64.StdEncoding.EncodeToString(u[:])
}
