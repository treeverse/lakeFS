package gs_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/gs"
	"github.com/treeverse/lakefs/pkg/config"
)

// testGoogleAccessID is a fake Google Access ID used for testing signed URLs
const testGoogleAccessID = "fake@test-project.iam.gserviceaccount.com"

// testPrivateKey is a PEM-encoded RSA private key for testing signed URLs with fake-gcs-server.
// This is a test-only key generated with: `openssl genrsa 2048`
var testPrivateKey = []byte(`-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0LBKd6cf913PB
GIsh9qfrBT2limGijI8ctSQCH7CrbEjCGI4gtyUhgxKaFMV9hKeAAca9Kilck+xH
rjBM2kjzTHlkVgWa3TNy3VM2v26DsK9Q8v6p2enMLE6ofqWTyyNaaSDXuXVfNKGg
vzahyIUp7kZG1f8HKuBIybZk74gTCubYF4wNZQ/asJuq+o7QGTQpK6v4SfdQtwxM
6w0bRDc737M7WJLNn0r2dF4hOytQW7cO9vX02GrW94P3j+N5tT2ktrULo4XQxTZO
uA6DawWGRg2jf1hsZ2aiOJdUU71FBx9iU7Z9tL4QyB29TOPxi4OugK3NMWlMhv/G
93/feUHRAgMBAAECggEAB3KWcEC2ilhJJ0YEKKVf49EZxHbjyg1gyY/1zaDWeQGP
AOH1DGZnAnt+7c+rvwsNmvbyf9rcg+sz7khwTpmhKsMiS0rbLLTV1dAkX/waCFd0
fxu/pJEBecsqPbYNfV5dZzUhsnUkDvP3oJPNi/K5tBs5ZwpybNm21MoXcBTu2qhB
RvQ3IvhcQj2PUfPw9S7kx5bCtW9aLcn5u7ySv6WNRBPbaEiMQhCEpFNQuG9w3keR
tJiQUzgvUZlutnyczMU+EZchmSHMUQMyE6ah+QStiyyUtOC1vtV95ZvLboEc6NJk
sPmx8ESugdj3Tx8PSciRD5T4C2RmktVJpxYaGEHauQKBgQDysPnIHEPsV7OjviZu
UfblYWGiWol/MRHj8/TKIGYU/Ss1qXMsICLudq/sghBmYA0h6vN1i44RR1w1HtOU
8urv7u90CCFigfu28UzPl2ajTkLbbg9wQwQ6qundcMSScSVEu9xRUrVkJToy5zuM
itl5yOSavusdOvWwZlFaqiTQeQKBgQC+DWvaE4z+reVPghVyD4Ob76DW81eGoq0Y
6JIMKnZqETVNEzWjWcsPF5dE747gmxtapHRvQrjrz0hfNttQgp7VcPbyLVx6MXrK
qL/wqLpZpCx8yJ5MQUHe6a+DJGSJeFH1nEZ2Bw6aOjoOD2GvdPp6flDytwrDMXZM
9cVbIwqWGQKBgQDn/jVIDX0AmHWouUSTgNa7PvPN9y4o4AdyGOqPrZjnx3teuLTY
IYBC5EIXm92Bf6AOJELGwrjz23tRbD5lzDC5W3abPIptWEP/BXufleMPiOhwSi2H
6whH7MnSXNIMCwzNP6fENYQgT1XrAw/xsWli+Z9OLeMi9hGWprhuKuc2QQKBgQCM
RnO4fn2u7MM4MBeMHI9TZUcd4HZV1XRV0jMZ761/FDx3KxqH+xq5hPwN0ZNvjIxg
Fsop5OGAi3orbN3rSr3ZZIugrIJ5XlP3iR5CjwccauS7JYhRWEk6MtlsvkvGe5xi
4HnRW9wXUarP/eJoErtd9iXhP+EduUBMBYspfW+u4QKBgF/kF/fucq2QkMCpDf+H
ITIxEup3Tg81lZAbtnZpfTU7TcPvgBRLWtg3gEKJfTIGL79hl7lPhdqUG9w5egl8
KOGyY30wiwFQ4Lu1MX+BZTKgQG9FVDtOQ43JQOLCbYdcYeKU9tJi7FUgvZmdZRaL
sGXLHjxoneUJGohAIXVzlIGW
-----END PRIVATE KEY-----`)

func newAdapter(opts ...gs.AdapterOption) *gs.Adapter {
	return gs.NewAdapter(client, opts...)
}

func TestAdapter(t *testing.T) {
	basePath, err := url.JoinPath("gs://", bucketName)
	require.NoError(t, err)
	localPath, err := url.JoinPath(basePath, "lakefs")
	require.NoError(t, err)
	externalPath, err := url.JoinPath(basePath, "external")
	require.NoError(t, err)

	adapter := newAdapter(
		gs.WithNowFactory(blocktest.NowMockDefault),
		gs.WithPresignedCredentials(testGoogleAccessID, testPrivateKey),
	)
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
	ctx := t.Context()
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
