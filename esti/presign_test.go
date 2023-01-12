package esti

import (
	"net/url"
	"strings"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
)

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
}
