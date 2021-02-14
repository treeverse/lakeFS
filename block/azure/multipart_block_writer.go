package azure

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/treeverse/lakefs/block"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type MultipartBlockWriter struct {
	reader *block.HashingReader // the reader that would be passed to copyFromReader, this is needed in order to get size and md5
	// to is the location we are writing our chunks to.
	to      azblob.BlockBlobURL
	toIDs   azblob.BlockBlobURL
	toSizes azblob.BlockBlobURL
	etag    string
}

func NewMultipartBlockWriter(reader *block.HashingReader, to, toIDs, toSizes azblob.BlockBlobURL) *MultipartBlockWriter {
	return &MultipartBlockWriter{
		reader:  reader,
		to:      to,
		toIDs:   toIDs,
		toSizes: toSizes,
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
