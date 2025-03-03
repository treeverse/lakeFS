package esti

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
)

func TestDownloadMetadataObject(t *testing.T) {
	ctx := context.Background()

	repo := createRepositoryUnique(ctx, t)
	UploadFileRandomData(ctx, t, repo, mainBranch, "some/random/path/43543985430548930", nil)
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "committing just to get a meta range!",
	})
	require.NoError(t, err, "failed to commit changes")
	require.NotNil(t, commitResp.JSON201)
	metarangeId := commitResp.JSON201.MetaRangeId

	// download meta-range
	response, err := client.GetMetadataObjectWithResponse(ctx, repo, "meta_range", metarangeId, &apigen.GetMetadataObjectParams{})
	if err != nil {
		t.Errorf("got unexpected error downloading metarange")
	}
	// try reading the meta-range
	iter, err := GravelerIterator(response.Body)
	if err != nil {
		t.Error("could not get an iterator from meta-range body")
	}
	if !iter.Next() {
		t.Error("should have at least one range")
	}
	record := iter.Value()
	gv, err := committed.UnmarshalValue(record.Value)
	if err != nil {
		t.Error("could not read range data")
	}
	rangeId := committed.ID(gv.Identity)

	// now try the range ID
	response, err = client.GetMetadataObjectWithResponse(ctx, repo, "range", string(rangeId), &apigen.GetMetadataObjectParams{})
	require.NoError(t, err, "failed to get range with presign=true")

	// try reading the range
	iter, err = GravelerIterator(response.Body)
	if err != nil {
		t.Error("could not get an iterator from range body")
	}
	if !iter.Next() {
		t.Error("should have at least one record")
	}
}
