package esti

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/thanhpk/randstr"

	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
)

func matchPreSignedURLContent(t *testing.T, preSignedURL, content string) {
	// Running an HTTP GET on the URL should return the object's content in body.
	r, err := http.Get(preSignedURL)
	require.NoErrorf(t, err, "failed to GET pre-signed url - %s", preSignedURL)

	retrievedData, err := io.ReadAll(r.Body)
	require.NoErrorf(t, err, "failed to read GET body from pre-signed url - %s", preSignedURL)
	require.Equal(t, string(retrievedData), content, "pre-signed url body doesn't match uploaded content")
	require.NoError(t, r.Body.Close(), "could not close response body")
}

func TestPreSign(t *testing.T) {
	SkipTestIfAskedTo(t)
	blockStoreType := viper.GetString("blockstore.type")
	expectedKey := ""
	switch blockStoreType {
	case block.BlockstoreTypeS3:
		expectedKey = "X-Amz-Signature"
	case block.BlockstoreTypeGS:
		expectedKey = "X-Goog-Signature"
	case block.BlockstoreTypeAzure:
		expectedKey = "sv"
	default:
		t.Skip("Only GS, S3 and Azure Blob supported for pre-signed urls")
	}
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "foo/bar", false)

	objContent := randstr.String(randomDataContentLength)
	_, err := uploadFileAndReport(ctx, repo, mainBranch, "foo/bar", objContent, false)
	if err != nil {
		t.Errorf("could no upload data file")
	}

	t.Run("preSignStat", func(t *testing.T) {
		response, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{
			Path:    "foo/bar",
			Presign: swag.Bool(true),
		})
		require.NoError(t, err, "failed to stat object with presign=true")
		require.NotNil(t, response.JSON200, "successful response")
		signedURL := response.JSON200.PhysicalAddress
		parsedSignedURL, err := url.Parse(signedURL)
		require.NoErrorf(t, err, "failed to parse url - %s", signedURL)
		require.Truef(t, strings.HasPrefix(parsedSignedURL.Scheme, "http"), "URL scheme http(s) - %s", signedURL)
		require.NotEmptyf(t, parsedSignedURL.Query().Get(expectedKey), "signature expected in key '%s' - %s", expectedKey, signedURL)
		matchPreSignedURLContent(t, signedURL, objContent)
	})

	t.Run("preSignList", func(t *testing.T) {
		paginationDelimiter := api.PaginationDelimiter("/")
		paginationPrefix := api.PaginationPrefix("foo/")
		response, err := client.ListObjectsWithResponse(ctx, repo, mainBranch, &api.ListObjectsParams{
			Prefix:    &paginationPrefix,
			Presign:   swag.Bool(true),
			Delimiter: &paginationDelimiter,
		})
		require.NoError(t, err, "failed to list objects with presign=true")
		require.NotNil(t, response.JSON200, "successful response")
		entry := response.JSON200.Results[0]
		signedURL := entry.PhysicalAddress
		parsedSignedURL, err := url.Parse(signedURL)
		require.NoErrorf(t, err, "failed to parse url - %s", signedURL)
		require.Truef(t, strings.HasPrefix(parsedSignedURL.Scheme, "http"), "URL scheme http(s) - %s", signedURL)
		require.NotEmptyf(t, parsedSignedURL.Query().Get(expectedKey), "signature expected in key '%s' - %s", expectedKey, signedURL)
		matchPreSignedURLContent(t, signedURL, objContent)
	})

	t.Run("preSignGet", func(t *testing.T) {
		response, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &api.GetObjectParams{
			Path:    "foo/bar",
			Presign: swag.Bool(true),
		})
		require.NoError(t, err, "failed to get object with presign=true")
		require.Equal(t, string(response.Body), objContent, "pre-signed url body doesn't match uploaded content")
		responseHost := response.HTTPResponse.Header.Get("Host")
		endpointParsedURL, err := url.Parse(endpointURL)
		require.NoError(t, err, "failed to parse the endpoint URL used by esti")
		require.NotEqual(t, endpointParsedURL.Host, responseHost, "Should have been redirected to the object store")
	})
}
