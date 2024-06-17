package block_test

import (
	"context"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestController_ValidateInterRegionStorage(t *testing.T) {
	ctx := context.Background()

	t.Run("namespace with the same region as the storage", func(t *testing.T) {
		opts := []testutil.MockAdapterOption{
			testutil.WithBlockstoreMetadata(&block.BlockstoreMetadata{Region: swag.String("us-west-2")}),
			testutil.WithNamespaceRegion("us-west-2"),
		}
		adapter := testutil.NewMockAdapter(opts...)

		err := block.ValidateInterRegionStorage(ctx, adapter, "s3://us-west-2/bucket-1")
		require.NoError(t, err)
	})

	t.Run("namespace region different from storage region", func(t *testing.T) {
		opts := []testutil.MockAdapterOption{
			testutil.WithBlockstoreMetadata(&block.BlockstoreMetadata{Region: swag.String("us-east-1")}),
			testutil.WithNamespaceRegion("us-west-2"),
		}
		adapter := testutil.NewMockAdapter(opts...)

		err := block.ValidateInterRegionStorage(ctx, adapter, "s3://us-west-2/bucket-1")
		require.ErrorIs(t, err, block.ErrInvalidNamespace)
	})
}
