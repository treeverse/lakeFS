package migrations

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/permissions"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func MigrateImportPermissions(ctx context.Context, kvStore kv.Store) error {
	const action = "fs:Import*"
	it, err := kv.NewPrimaryIterator(ctx, kvStore, (&model.PolicyData{}).ProtoReflect().Type(), model.PartitionKey, model.PolicyPath(""), kv.IteratorOptionsFrom([]byte("")))
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		update := false
		entry := it.Entry()
		policy := entry.Value.(*model.PolicyData)
		for _, statement := range policy.Statements {
			if slices.Contains(statement.Action, action) { // Avoid duplication
				continue
			}
			idx := slices.Index(statement.Action, permissions.ImportFromStorageAction)
			if idx >= 0 {
				statement.Action[idx] = action
				update = true
			}
		}

		if update {
			policy.CreatedAt = timestamppb.Now()
			if err = kv.SetMsg(ctx, kvStore, model.PartitionKey, entry.Key, policy); err != nil {
				return err
			}
		}
	}

	err = kv.SetDBSchemaVersion(ctx, kvStore, kv.ACLImportMigrateVersion)
	if err != nil {
		return fmt.Errorf("migration succeeded but failed to upgrade version, to fix this re-run migration: %w", err)
	}
	return nil
}
