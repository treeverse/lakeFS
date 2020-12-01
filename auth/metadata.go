package auth

import (
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type MetadataManager interface {
	SetupTimestamp() (time.Time, error)
	UpdateSetupTimestamp(time.Time) error
	Write() (map[string]string, error)
}

type DBMetadataManager struct {
	version string
	db      db.Database
}

const (
	InstallationIDKeyName = "installation_id"
	SetupTimestampKeyName = "setup_timestamp"
)

func NewDBMetadataManager(version string, database db.Database) *DBMetadataManager {
	return &DBMetadataManager{
		version: version,
		db:      database,
	}
}

func generateInstallationID() string {
	installationID := config.GetFixedInstallationID()
	if installationID == "" {
		installationID = uuid.New().String()
	}
	return installationID
}

func insertOrGetInstallationID(tx db.Tx) (string, error) {
	newInstallationID := generateInstallationID()
	res, err := tx.Exec(`INSERT INTO auth_installation_metadata (key_name, key_value)
			VALUES ($1,$2)
			ON CONFLICT DO NOTHING`,
		InstallationIDKeyName, newInstallationID)
	if err != nil {
		return "", err
	}
	affected := res.RowsAffected()
	if affected == 1 {
		return newInstallationID, nil
	}
	return getInstallationID(tx)
}

func getInstallationID(tx db.Tx) (string, error) {
	var installationID string
	err := tx.GetPrimitive(&installationID, `SELECT key_value FROM auth_installation_metadata WHERE key_name = $1`,
		InstallationIDKeyName)
	return installationID, err
}

func (d *DBMetadataManager) InstallationID() (string, error) {
	installationID, err := d.db.Transact(func(tx db.Tx) (interface{}, error) {
		return getInstallationID(tx)
	})
	return installationID.(string), err
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

func (d *DBMetadataManager) UpdateSetupTimestamp(ts time.Time) error {
	_, err := d.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, writeMetadata(tx, map[string]string{
			SetupTimestampKeyName: ts.UTC().Format(time.RFC3339),
		})
	}, db.WithLogger(logging.Dummy()))
	return err
}

func (d *DBMetadataManager) SetupTimestamp() (time.Time, error) {
	setupTimestamp, err := d.db.Transact(func(tx db.Tx) (interface{}, error) {
		return getSetupTimestamp(tx)
	}, db.WithLogger(logging.Dummy()), db.ReadOnly())
	if err != nil {
		return time.Time{}, err
	}
	return setupTimestamp.(time.Time), nil
}

func (d *DBMetadataManager) Write() (map[string]string, error) {
	metadata := make(map[string]string)
	metadata["lakefs_version"] = d.version
	metadata["golang_version"] = runtime.Version()
	metadata["architecture"] = runtime.GOARCH
	metadata["os"] = runtime.GOOS
	dbMeta, err := d.db.Metadata()
	if err == nil {
		for k, v := range dbMeta {
			metadata[k] = v
		}
	}
	_, err = d.db.Transact(func(tx db.Tx) (interface{}, error) {
		// write metadata
		err = writeMetadata(tx, metadata)
		if err != nil {
			return nil, err
		}
		// write installation id
		installationID, err := insertOrGetInstallationID(tx)
		if err == nil {
			metadata[InstallationIDKeyName] = installationID
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
