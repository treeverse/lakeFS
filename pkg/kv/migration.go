package kv

import (
	"context"
	"errors"
	"fmt"

	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Migrator interface {
	Migrate(ctx context.Context) error
}

type DatabaseMigrator struct {
	params kvparams.KV
}

func NewDatabaseMigrator(params kvparams.KV) *DatabaseMigrator {
	return &DatabaseMigrator{
		params: params,
	}
}

// Migrate TODO (niro): Currently just set up the KV DB Schema version. Need to create a migration flow for KV
func (d *DatabaseMigrator) Migrate(ctx context.Context) error {
	kvStore, err := Open(ctx, d.params)
	if err != nil {
		return fmt.Errorf("failed to open KV store: %w", err)
	}
	defer kvStore.Close()
	version, err := GetDBSchemaVersion(ctx, kvStore)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("failed to setup KV store: %w", err)
	}
	if version < InitialMigrateVersion { // 0 In case of ErrNotFound
		return SetDBSchemaVersion(ctx, kvStore, InitialMigrateVersion)
	}
	return nil
}

func ValidateSchemaVersion(ctx context.Context, store Store, migrationRequired bool) error {
	kvVersion, err := GetDBSchemaVersion(ctx, store)
	if errors.Is(err, ErrNotFound) && !migrationRequired {
		logging.Default().Info("No KV Schema version, setup required")
		return nil
	}
	if err != nil {
		return fmt.Errorf("get KV schema version: %w", err)
	}

	if kvVersion < InitialMigrateVersion {
		if migrationRequired {
			return fmt.Errorf("migration to KV required, for more information see https://docs.lakefs.io/reference/upgrade.html#lakefs-0800-or-greater-kv-migration : %w", err)
		}
		return fmt.Errorf("(scehma version %d): %w", kvVersion, ErrInvalidSchemaVersion)
	}

	logging.Default().WithField("version", kvVersion).Info("KV valid")
	return nil
}
