package esti

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
)

func TestPreSign(t *testing.T) {
	SkipTestIfAskedTo(t)
	blockStoreType := viper.GetViper().GetString("blockstore_type")
	expected := ""
	switch blockStoreType {
	case block.BlockstoreTypeS3:
		expected = "X-Amz-Signature="
	case block.BlockstoreTypeGS:
		expected = "X-Goog-Signature="
	case block.BlockstoreTypeAzure:
		expected = "sv="
	default:
		t.Skip("Only GS, S3 and Azure Blob supported for pre-signed urls")
	}

	ctx, _, repo := setupTest(t)
	defer tearDownTest(repo)
	_, _ = uploadFileRandomData(ctx, t, repo, mainBranch, "foo/bar", true)

	// Get Object
	preSign := true
	response, err := client.StatObjectWithResponse(ctx, repo, mainBranch, &api.StatObjectParams{
		Path:    "foo/bar",
		Presign: &preSign,
	})
	require.NoError(t, err, "failed to stat object with presign=true")
	signedUrl := response.JSON200.PhysicalAddress
	if !strings.HasPrefix(signedUrl, "http") {
		t.Errorf("exected an HTTP(s) URL instead of an object store URI, got %s", signedUrl)
	}

	if !strings.Contains(signedUrl, expected) {
		t.Errorf("expected a signed URL, got %s", signedUrl)
	}
}
