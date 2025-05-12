package gs_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"io"
	"net/url"
	"regexp"
	"testing"

	"encoding/hex"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/gs"
	"github.com/treeverse/lakefs/pkg/config"
)

func newAdapter() *gs.Adapter {
	return gs.NewAdapter(client)
}

func TestAdapter(t *testing.T) {
	basePath, err := url.JoinPath("gs://", bucketName)
	require.NoError(t, err)
	localPath, err := url.JoinPath(basePath, "lakefs")
	require.NoError(t, err)
	externalPath, err := url.JoinPath(basePath, "external")
	require.NoError(t, err)

	adapter := newAdapter()
	defer func() {
		require.NoError(t, adapter.Close())
	}()

	blocktest.AdapterTest(t, adapter, localPath, externalPath)
}

func TestAdapterNamespace(t *testing.T) {
	adapter := newAdapter()
	defer func() {
		require.NoError(t, adapter.Close())
	}()

	expr, err := regexp.Compile(adapter.GetStorageNamespaceInfo(config.SingleBlockstoreID).ValidityRegex)
	require.NoError(t, err)

	tests := []struct {
		Name      string
		Namespace string
		Success   bool
	}{
		{
			Name:      "valid_path",
			Namespace: "gs://bucket/path/to/repo1",
			Success:   true,
		},
		{
			Name:      "double_slash",
			Namespace: "gs://bucket/path//to/repo1",
			Success:   true,
		},
		{
			Name:      "invalid_schema",
			Namespace: "s3:/test/adls/core/windows/net",
			Success:   false,
		},
		{
			Name:      "invalid_path",
			Namespace: "https://test/adls/core/windows/net",
			Success:   false,
		},
		{
			Name:      "invalid_string",
			Namespace: "this is a bad string",
			Success:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			require.Equal(t, tt.Success, expr.MatchString(tt.Namespace))
		})
	}
}
func TestMultipartUploadWithMD5(t *testing.T) {
	ctx := context.Background()
	adapter := newAdapter()
	defer require.NoError(t, adapter.Close())

	content := []byte("test content for multipart upload")
	obj := block.ObjectPointer{
		StorageNamespace: "gs://" + bucketName,
		Identifier:       "test-object",
		IdentifierType:   block.IdentifierTypeRelative,
	}

	// Create multipart upload
	createResp, err := adapter.CreateMultiPartUpload(ctx, obj, nil, block.CreateMultiPartUploadOpts{})
	require.NoError(t, err)
	uploadID := createResp.UploadID
	// Upload first part
	partResp1, err := adapter.UploadPart(ctx, obj, int64(len(content[:10])), bytes.NewReader(content[:10]), uploadID, 1)
	require.NoError(t, err)
	require.Equal(t, calcETag(content[:10]), partResp1.ETag)

	// Upload second part
	partResp2, err := adapter.UploadPart(ctx, obj, int64(len(content[10:])), bytes.NewReader(content[10:]), uploadID, 2)
	require.NoError(t, err)
	require.Equal(t, calcETag(content[10:]), partResp2.ETag)

	// Complete multipart upload
	completion := &block.MultipartUploadCompletion{
		Part: []block.MultipartPart{
			{PartNumber: 1, ETag: partResp1.ETag},
			{PartNumber: 2, ETag: partResp2.ETag},
		},
	}
	completeResp, err := adapter.CompleteMultiPartUpload(ctx, obj, uploadID, completion)
	require.NoError(t, err)
	require.NotEmpty(t, completeResp.ETag)

	// Verify uploaded content
	reader, err := adapter.Get(ctx, obj)
	require.NoError(t, err)
	defer reader.Close()

	readContent, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, content, readContent)
}

func calcETag(data []byte) string {
	etag := md5.Sum(data) //nolint:gosec
	return hex.EncodeToString(etag[:])
}
