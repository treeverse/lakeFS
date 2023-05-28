package migrations

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/permissions"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func MigrateImportPermissions(ctx context.Context, kvStore kv.Store, cfg *config.Config) error {
	// skip migrate for users with External authorizations
	if !cfg.IsAuthUISimplified() {
		fmt.Println("skipping ACL migration - external Authorization")
		return updateKVSchemaVersion(ctx, kvStore, kv.ACLImportMigrateVersion)
	}

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

	return updateKVSchemaVersion(ctx, kvStore, kv.ACLImportMigrateVersion)
}
