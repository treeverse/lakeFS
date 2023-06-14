package migrations

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/kv"
)

func updateKVSchemaVersion(ctx context.Context, kvStore kv.Store, version uint) error {
	err := kv.SetDBSchemaVersion(ctx, kvStore, version)
	if err != nil {
		return fmt.Errorf("failed to upgrade version, to fix this re-run migration: %w", err)
	}
	return nil
}
