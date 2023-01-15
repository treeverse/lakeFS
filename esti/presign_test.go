package esti

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
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
	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)

	// look at the storage namespace to make sure our repo is indeed running with a supported object store
	repoResponse, err := client.GetRepositoryWithResponse(ctx, repo)
	require.NoError(t, err, "could not get repository information")
	require.Equal(t, repoResponse.StatusCode(), http.StatusOK, "could not get repository information")
	namespace := repoResponse.JSON200.StorageNamespace
	blockStoreType, _, _ := strings.Cut(namespace, ":")
	expectedKey := ""
	switch blockStoreType {
	case block.BlockstoreTypeS3:
		expectedKey = "X-Amz-Signature"
	case block.BlockstoreTypeGS:
		expectedKey = "X-Goog-Signature"
	case block.BlockstoreTypeAzure:
		expectedKey = "sv"
	default:
		t.Skipf("Only GS, S3 and Azure Blob supported for pre-signed urls. Got: %s", blockStoreType)
	}

	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "foo/bar", false)

	objContent := randstr.String(randomDataContentLength)
	_, err = uploadFileAndReport(ctx, repo, mainBranch, "foo/bar", objContent, false)
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

	t.Run("preSignGetPhysicalAddress", func(t *testing.T) {
		// request a pre-signed URL for us to upload to
		response, err := client.GetPhysicalAddressWithResponse(ctx, repo, mainBranch, &api.GetPhysicalAddressParams{
			Path:    "foo/uploaded",
			Presign: swag.Bool(true),
		})
		require.NoError(t, err, "failed to get physical address with presign=true")
		require.Equal(t, response.StatusCode(), http.StatusOK, "expected a 200 OK")
		preSignedURLPtr := response.JSON200.PresignedUrl
		require.NotEmpty(t, preSignedURLPtr)
		preSignedURL := *preSignedURLPtr

		// upload to the pre-signed URL (Assuming that all object stores do it with a simple HTTP PUT
		uploadContentLength := 1024 * 1024 // 1MB
		objContent := randstr.String(uploadContentLength)
		req, err := http.NewRequest(http.MethodPut, preSignedURL, bytes.NewReader([]byte(objContent)))
		require.NoError(t, err, "failed to create PUT request")
		req.Header.Set("Content-Type", "application/octet-stream")
		httpResp, err := http.DefaultClient.Do(req)
		require.NoError(t, err, "failed to execute PUT request")
		require.Truef(t, httpResp.StatusCode < 400, "got a bad response from pre-signed URL for PUT: %s", httpResp.Status)

		// Let's link this physical address
		linkResponse, err := client.LinkPhysicalAddressWithResponse(ctx, repo, mainBranch, &api.LinkPhysicalAddressParams{
			Path: "foo/uploaded",
		}, api.LinkPhysicalAddressJSONRequestBody{
			Checksum:    httpResp.Header.Get("Etag"),
			ContentType: swag.String("application/octet-stream"),
			SizeBytes:   int64(uploadContentLength),
			Staging: api.StagingLocation{
				PhysicalAddress: response.JSON200.PhysicalAddress,
				PresignedUrl:    response.JSON200.PresignedUrl,
				Token:           response.JSON200.Token,
			},
		})
		require.NoError(t, err, "failed to link physical address")
		require.Equalf(t, linkResponse.StatusCode(), http.StatusOK, "unexpected HTTP code for link_physical_address: %s", linkResponse.Status())

		// Finally, let's read it back and see that we get back what we uploaded!
		readBackResponse, err := client.GetObjectWithResponse(ctx, repo, mainBranch, &api.GetObjectParams{
			Path: "foo/uploaded",
		})
		require.NoError(t, err, "failed to read back linked object")
		require.Equalf(t, readBackResponse.StatusCode(), http.StatusOK, "unexpected HTTP code for get_object after linking: %s", readBackResponse.Status())
		returnedContent := readBackResponse.Body
		require.Equal(t, string(returnedContent), objContent, "the body returned is different from the one uploaded")
	})
}
