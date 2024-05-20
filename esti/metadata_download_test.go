package esti

import (
	"context"
	"testing"

	pebblesst "github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/pkg/graveler/sstable"

	"github.com/treeverse/lakefs/pkg/graveler/committed"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

func gravelerIterator(data []byte) (*sstable.Iterator, error) {
	// read file descriptor
	reader, err := pebblesst.NewMemReader(data, pebblesst.ReaderOptions{})
	if err != nil {
		return nil, err
	}

	// create an iterator over the whole thing
	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}

	// wrap it in a Graveler iterator
	dummyDeref := func() error { return nil }
	return sstable.NewIterator(iter, dummyDeref), nil
}

func TestDownloadMetadataObject(t *testing.T) {
	ctx := context.Background()
	const numOfRepos = 5

	repo := createRepositoryUnique(ctx, t)
	uploadFileRandomData(ctx, t, repo, mainBranch, "some/random/path/43543985430548930")
	commitResp, err := client.CommitWithResponse(ctx, repo, mainBranch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
		Message: "committing just to get a meta range!",
	})
	require.NoError(t, err, "failed to commit changes")
	metarangeId := commitResp.JSON201.MetaRangeId

	// download meta-range
	response, err := client.GetMetadataObjectWithResponse(ctx, repo, "meta_range", metarangeId, &apigen.GetMetadataObjectParams{})
	if err != nil {
		t.Errorf("got unexpected error downloading metarange")
	}
	// try reading the meta-range
	iter, err := gravelerIterator(response.Body)
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
	iter, err = gravelerIterator(response.Body)
	if err != nil {
		t.Error("could not get an iterator from range body")
	}
	if !iter.Next() {
		t.Error("should have at least one record")
	}
}
