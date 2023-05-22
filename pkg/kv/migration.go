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

var (
	ErrMigrationVersion  = errors.New("wrong kv version")
	ErrMigrationRequired = errors.New("migration required")
)

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
		return SetDBSchemaVersion(ctx, kvStore, NextSchemaVersion-1)
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
	switch {
	case kvVersion >= NextSchemaVersion:
		return kvVersion, fmt.Errorf("incompatible schema version. Latest: %d: %w", NextSchemaVersion-1, ErrMigrationVersion)
	case kvVersion < InitialMigrateVersion:
		return kvVersion, fmt.Errorf("migration to KV required. Did you migrate using version v0.80.x? https://docs.lakefs.io/reference/upgrade.html#lakefs-0800-or-greater-kv-migration: %w", ErrMigrationVersion)
	case kvVersion < ACLNoReposMigrateVersion:
		return kvVersion, fmt.Errorf("migration to ACL required. Did you migrate using version v0.99.x? https://docs.lakefs.io/reference/access-control-list.html#migrating-from-the-previous-version-of-acls: %w", ErrMigrationVersion)
	case kvVersion < ACLImportMigrateVersion:
		return kvVersion, fmt.Errorf("ACL migration required. Please run 'lakefs migrate up': %w", ErrMigrationRequired)
	}

	logging.Default().WithField("version", kvVersion).Info("KV valid")
	return kvVersion, nil
}
