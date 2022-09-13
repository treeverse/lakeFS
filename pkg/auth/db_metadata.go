package auth

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"time"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

type DBMetadataManager struct {
	version        string
	installationID string
	db             db.Database
}

func NewDBMetadataManager(version string, fixedInstallationID string, database db.Database) *DBMetadataManager {
	return &DBMetadataManager{
		version:        version,
		installationID: generateInstallationID(fixedInstallationID),
		db:             database,
	}
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

func (d *DBMetadataManager) IsInitialized(ctx context.Context) (bool, error) {
	setupTimestamp, err := d.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return getSetupTimestamp(tx)
	}, db.WithLogger(logging.Dummy()), db.ReadOnly())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.Is(err, pgx.ErrNoRows) ||
			(errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UndefinedTable) {
			return false, nil
		}

		return false, err
	}

	return !setupTimestamp.(time.Time).IsZero(), nil
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

func exportMetadata(ctx context.Context, d *pgxpool.Pool, je *json.Encoder) error {
	type metadataEntry struct {
		KeyName  string `db:"key_name"`
		KeyValue string `db:"key_value"`
	}

	// Gathering DB metadata to exclude from export
	pgdb := db.NewPgxDatabase(d)
	dbMeta, err := pgdb.Metadata(ctx)
	if err != nil {
		return err
	}

	rows, err := d.Query(ctx, "SELECT key_name, key_value from auth_installation_metadata")
	if err != nil {
		return err
	}
	defer rows.Close()
	scanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		entry := metadataEntry{}
		err := scanner.Scan(&entry)
		if err != nil {
			return err
		}
		if _, exist := dbMeta[entry.KeyName]; exist {
			// DB metadata - skip
			continue
		}
		key := model.MetadataKeyPath(entry.KeyName)
		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(model.PartitionKey),
			Key:          []byte(key),
			Value:        []byte(entry.KeyValue),
		}); err != nil {
			return err
		}
	}
	return nil
}
