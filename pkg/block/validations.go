package block

import (
	"context"
	"errors"
	"fmt"
)

func ValidateInterRegionStorage(ctx context.Context, adapter Adapter, storageID, storageNamespace string) error {
	blockstoreMetadata, err := adapter.BlockstoreMetadata(ctx, storageID)
	if errors.Is(err, ErrOperationNotSupported) {
		// region detection not supported for the server's blockstore, skip validation
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get blockstore region: %w", err)
	}

	bucketRegion, err := adapter.GetRegion(ctx, storageID, storageNamespace)
	if err != nil {
		return fmt.Errorf("failed to get region of storage namespace %s: %w", storageNamespace, ErrInvalidNamespace)
	}

	blockstoreRegion := *blockstoreMetadata.Region
	if blockstoreRegion != bucketRegion {
		return fmt.Errorf(`%w: namespace region ("%s") does not match block region ("%s")`, ErrInvalidNamespace, bucketRegion, blockstoreRegion)
	}

	return nil
}
