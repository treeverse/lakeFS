package auth

import (
	"context"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/logging"
)

type MetadataManager interface {
	SetupTimestamp(context.Context) (time.Time, error)
	UpdateSetupTimestamp(context.Context, time.Time) error
	Write(context.Context) (map[string]string, error)
}

type DBMetadataManager struct {
	version string
	installationID string
	db      db.Database
}

const (
	InstallationIDKeyName = "installation_id"
	SetupTimestampKeyName = "setup_timestamp"
)

func NewDBMetadataManager(version string, fixedInstallationID string, database db.Database) *DBMetadataManager {
	return &DBMetadataManager{
		version: version,
		installationID: generateInstallationID(fixedInstallationID),
		db:      database,
	}
}

func generateInstallationID(installationID string) string {
	if installationID == "" {
		installationID = uuid.New().String()
	}
	return installationID
}

func insertOrGetInstallationID(tx db.Tx, installationID string) (string, error) {
	res, err := tx.Exec(`INSERT INTO auth_installation_metadata (key_name, key_value)
			VALUES ($1,$2)
			ON CONFLICT DO NOTHING`,
		InstallationIDKeyName, installationID)
	if err != nil {
		return "", err
	}
	affected := res.RowsAffected()
	if affected == 1 {
		return installationID, nil
	}
	return getInstallationID(tx)
}

func getInstallationID(tx db.Tx) (string, error) {
	var installationID string
	err := tx.GetPrimitive(&installationID, `SELECT key_value FROM auth_installation_metadata WHERE key_name = $1`,
		InstallationIDKeyName)
	return installationID, err
}

func getSetupTimestamp(tx db.Tx) (time.Time, error) {
	var value string
	err := tx.GetPrimitive(&value, `SELECT key_value FROM auth_installation_metadata WHERE key_name = $1`,
		SetupTimestampKeyName)
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, value)
}

func writeMetadata(tx db.Tx, items map[string]string) error {
	for key, value := range items {
		_, err := tx.Exec(`
			INSERT INTO auth_installation_metadata (key_name, key_value)
			VALUES ($1, $2)
			ON CONFLICT (key_name) DO UPDATE set key_value = $2`,
			key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DBMetadataManager) UpdateSetupTimestamp(ctx context.Context, ts time.Time) error {
	_, err := d.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, writeMetadata(tx, map[string]string{
			SetupTimestampKeyName: ts.UTC().Format(time.RFC3339),
		})
	}, db.WithLogger(logging.Dummy()))
	return err
}

func (d *DBMetadataManager) SetupTimestamp(ctx context.Context) (time.Time, error) {
	setupTimestamp, err := d.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return getSetupTimestamp(tx)
	}, db.WithLogger(logging.Dummy()), db.ReadOnly())
	if err != nil {
		return time.Time{}, err
	}
	return setupTimestamp.(time.Time), nil
}

func (d *DBMetadataManager) Write(ctx context.Context) (map[string]string, error) {
	metadata := make(map[string]string)
	metadata["lakefs_version"] = d.version
	metadata["golang_version"] = runtime.Version()
	metadata["architecture"] = runtime.GOARCH
	metadata["os"] = runtime.GOOS
	dbMeta, err := d.db.Metadata(ctx)
	if err == nil {
		for k, v := range dbMeta {
			metadata[k] = v
		}
	}
	_, err = d.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		// write metadata
		err = writeMetadata(tx, metadata)
		if err != nil {
			return nil, err
		}
		// write installation id
		d.installationID, err = insertOrGetInstallationID(tx, d.installationID)
		if err == nil {
			metadata[InstallationIDKeyName] = d.installationID
		}

		// get setup timestamp
		setupTS, err := getSetupTimestamp(tx)
		if err == nil {
			metadata[SetupTimestampKeyName] = setupTS.UTC().Format(time.RFC3339)
		}
		return nil, nil
	}, db.WithLogger(logging.Dummy()))
	return metadata, err
}
