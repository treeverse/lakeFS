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

var errMigrationRequired = errors.New("wrong kv version")

func ValidateSchemaVersion(ctx context.Context, store Store) error {
	kvVersion, err := GetDBSchemaVersion(ctx, store)
	if errors.Is(err, ErrNotFound) {
		// probably a new installation
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("get KV schema version: %w", err)
	}
	if kvVersion < InitialMigrateVersion {
		logging.Default().Info("Migration to KV required. Did you migrate using version v0.80.x? https://docs.lakefs.io/reference/upgrade.html#lakefs-0800-or-greater-kv-migration")
		return errMigrationRequired
	}

	logging.Default().WithField("version", kvVersion).Info("KV valid")
	return nil
}
