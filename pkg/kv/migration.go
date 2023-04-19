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
	params kvparams.Config
}

var ErrMigrationRequired = errors.New("wrong kv version")

func NewDatabaseMigrator(params kvparams.Config) *DatabaseMigrator {
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
		return SetDBSchemaVersion(ctx, kvStore, ACLNoReposMigrateVersion)
	}
	return nil
}

func ValidateSchemaVersion(ctx context.Context, store Store) (int, error) {
	kvVersion, err := GetDBSchemaVersion(ctx, store)
	if errors.Is(err, ErrNotFound) {
		// probably a new installation
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("get KV schema version: %w", err)
	}
	if kvVersion < InitialMigrateVersion {
		logging.Default().Info("Migration to KV required. Did you migrate using version v0.80.x? https://docs.lakefs.io/reference/upgrade.html#lakefs-0800-or-greater-kv-migration")
		return 0, ErrMigrationRequired
	}
	if kvVersion < ACLNoReposMigrateVersion {
		logging.Default().Info("Migration to ACL required. Did you migrate using version v0.98.x? https://docs.lakefs.io/reference/access-control-list.html#migrating-from-the-previous-version-of-acls")
		return 0, ErrMigrationRequired
	}

	logging.Default().WithField("version", kvVersion).Info("KV valid")
	return kvVersion, nil
}
