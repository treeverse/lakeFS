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
	err = SetDBSchemaVersion(ctx, kvStore, InitialMigrateVersion)
	if err != nil {
		return fmt.Errorf("failed to setup KV store: %w", err)
	}
	return nil
}

func ValidateSchemaVersion(ctx context.Context, store Store, migrationRequired bool) error {
	kvVersion, err := GetDBSchemaVersion(ctx, store)
	switch {
	case errors.Is(err, ErrNotFound):
		if migrationRequired {
			return fmt.Errorf("missing KV schema version: %w", err)
		} else {
			logging.Default().Debug("No KV Schema version, setup required")
			return nil
		}

	case err != nil:
		return fmt.Errorf("get KV schema version: %w", err)

	case kvVersion < InitialMigrateVersion:
		if migrationRequired {
			return fmt.Errorf("migration required, for more information see https://docs.lakefs.io/deploying-aws/upgrade.html: %w", err)
		} else {
			return fmt.Errorf("missing KV schema version (%d): %w", kvVersion, err)
		}
	}
	return nil
}
