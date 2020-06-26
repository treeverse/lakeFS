package db

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/jmoiron/sqlx"
)

type TxFunc func(tx Tx) (interface{}, error)

type Database interface {
	io.Closer
	Transact(fn TxFunc, opts ...TxOpt) (interface{}, error)
	Metadata() (map[string]string, error)
}

type SqlxDatabase struct {
	db *sqlx.DB
}

func NewSqlxDatabase(db *sqlx.DB) *SqlxDatabase {
	return &SqlxDatabase{db: db}
}

func (d *SqlxDatabase) Close() error {
	return d.db.Close()
}

func (d *SqlxDatabase) Transact(fn TxFunc, opts ...TxOpt) (interface{}, error) {
	options := DefaultTxOptions()
	for _, opt := range opts {
		opt(options)
	}
	var attempt int
	var ret interface{}
	for attempt < SerializationRetryMaxAttempts {
		if attempt > 0 {
			duration := time.Duration(int(SerializationRetryStartInterval) * attempt)
			options.logger.
				WithField("attempt", attempt).
				WithField("sleep_interval", duration).
				Warn("retrying transaction due to serialization error")
			time.Sleep(duration)
		}

		tx, err := d.db.BeginTxx(options.ctx, &sql.TxOptions{
			Isolation: options.isolationLevel,
			ReadOnly:  options.readOnly,
		})
		if err != nil {
			return nil, err
		}
		ret, err = fn(&dbTx{tx: tx, logger: options.logger})
		if err != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				return nil, rollbackErr
			}
			// retry on serialization error
			if IsSerializationError(err) {
				// retry
				attempt++
				continue
			}
			return nil, err
		} else {
			err = tx.Commit()
			if err != nil {
				// retry on serialization error
				if IsSerializationError(err) {
					attempt++
					continue
				}
				// other commit error
				return nil, err
			}
			// committed successfully, we're done
			return ret, nil
		}
	}

	return nil, ErrSerialization
}

func (d *SqlxDatabase) Metadata() (map[string]string, error) {
	metadata := make(map[string]string)
	version, err := d.getVersion()
	if err == nil {
		metadata["postgresql_version"] = version
	}
	auroraVersion, err := d.getAuroraVersion()
	if err == nil {
		metadata["postgresql_aurora_version"] = auroraVersion
	}

	m, err := d.Transact(func(tx Tx) (interface{}, error) {
		metadata := make(map[string]string)

		// select name,setting from pg_settings
		// where name in ('data_directory', 'rds.extensions', 'TimeZone', 'work_mem')
		type pgSettings struct {
			Name    string `db:"name"`
			Setting string `db:"setting"`
		}
		pgs := make([]pgSettings, 0)
		err = tx.Select(&pgs,
			`SELECT name, setting FROM pg_settings
					WHERE name IN ('data_directory', 'rds.extensions', 'TimeZone', 'work_mem')`)
		if err == nil {
			for _, setting := range pgs {
				if setting.Name == "data_directory" {
					if strings.HasPrefix(setting.Setting, "/rdsdata") {
						metadata["postgresql_setting_is_rds"] = "true"
					} else {
						metadata["postgresql_setting_is_rds"] = "false"
					}
					continue
				}
				metadata[fmt.Sprintf("postgresql_setting_%s", setting.Name)] = setting.Setting
			}
		}

		return metadata, nil

	}, ReadOnly())
	if err == nil {
		settings := m.(map[string]string)
		for k, v := range settings {
			metadata[k] = v
		}
	}

	return metadata, nil
}

func (d *SqlxDatabase) getVersion() (string, error) {
	v, err := d.Transact(func(tx Tx) (interface{}, error) {
		type ver struct {
			Version string `db:"version"`
		}
		var v ver
		err := tx.Get(&v, "SELECT version()")
		if err != nil {
			return "", err
		}
		return v.Version, nil
	}, ReadOnly(), WithLogger(logging.Dummy()))
	if err != nil {
		return "", err
	}
	return v.(string), err

}

func (d *SqlxDatabase) getAuroraVersion() (string, error) {
	v, err := d.Transact(func(tx Tx) (interface{}, error) {
		type ver struct {
			Version string `db:"aurora_version"`
		}
		var v ver
		err := tx.Get(&v, "SELECT aurora_version()")
		if err != nil {
			return "", err
		}
		return v.Version, nil
	}, ReadOnly(), WithLogger(logging.Dummy()))
	if err != nil {
		return "", err
	}
	return v.(string), err

}
