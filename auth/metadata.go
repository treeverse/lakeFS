package auth

import (
	"errors"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
)

type MetadataManager interface {
	InstallationID() (string, error)
	Write() (map[string]string, error)
}

type DBMetadataManager struct {
	version string
	db      db.Database
}

func NewDBMetadataManager(version string, database db.Database) *DBMetadataManager {
	return &DBMetadataManager{
		version: version,
		db:      database,
	}
}

func getInstallationID(tx db.Tx) (string, error) {
	const InstallationIDKeyName = "installation_id"
	var installationID string
	err := tx.Get(&installationID, `SELECT key_value FROM auth_installation_metadata WHERE key_name = $1`,
		InstallationIDKeyName)
	return installationID, err
}

func writeMetadata(tx sqlx.Execer, items map[string]string) error {
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

func (d *DBMetadataManager) InstallationID() (string, error) {
	installationID, err := d.db.Transact(func(tx db.Tx) (interface{}, error) {
		return getInstallationID(tx)
	}, db.ReadOnly())
	if err != nil {
		return "", err
	}
	return installationID.(string), nil
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

	// see if we have existing metadata or we need to generate one
	_, err = d.db.Transact(func(tx db.Tx) (interface{}, error) {
		// get installation ID - if we don't have one we'll generate one
		_, err := getInstallationID(tx)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			return nil, err
		}

		if err != nil { // i.e. err is db.ErrNotFound
			// we don't have an installation ID - let's write one.
			installationID := uuid.Must(uuid.NewUUID()).String()
			metadata["installation_id"] = installationID
			metadata["setup_time"] = time.Now().UTC().Format(time.RFC3339)
		}

		err = writeMetadata(tx, metadata)
		return nil, err
	})

	return metadata, err
}
