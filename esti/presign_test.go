package esti

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/treeverse/lakefs/pkg/api"
)

func TestPreSign(t *testing.T) {
	SkipTestIfAskedTo(t)
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

	if !strings.Contains(signedUrl, "X-Amz-Signature=") {
		t.Errorf("expected a signed URL, got %s", signedUrl)
	}
}
