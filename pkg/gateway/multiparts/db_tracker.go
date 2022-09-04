package multiparts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4/pgxpool"
	blockparams "github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/kv"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/version"
	"google.golang.org/protobuf/proto"
)

//nolint:gochecknoinits
func init() {
	kvpg.RegisterMigrate(packageName, Migrate, []string{"gateway_multiparts"})
}

func (m Metadata) Set(k, v string) {
	m[strings.ToLower(k)] = v
}

func (m Metadata) Get(k string) string {
	return m[strings.ToLower(k)]
}
func (m Metadata) Value() (driver.Value, error) {
	return json.Marshal(m)
}

func (m *Metadata) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	data, ok := src.([]byte)
	if !ok {
		return ErrInvalidMetadataSrcFormat
	}
	return json.Unmarshal(data, m)
}

type dbTracker struct {
	db db.Database
}

func NewDBTracker(adb db.Database) Tracker {
	return &dbTracker{
		db: adb,
	}
}

func (m *dbTracker) Create(ctx context.Context, multipart MultipartUpload) error {
	if multipart.UploadID == "" {
		return ErrInvalidUploadID
	}
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`INSERT INTO gateway_multiparts (upload_id,path,creation_date,physical_address, metadata, content_type)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			multipart.UploadID, multipart.Path, multipart.CreationDate, multipart.PhysicalAddress, multipart.Metadata, multipart.ContentType)
		return nil, err
	})
	return err
}

func (m *dbTracker) Get(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	if uploadID == "" {
		return nil, ErrInvalidUploadID
	}
	res, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		var m MultipartUpload
		if err := tx.Get(&m, `
			SELECT upload_id, path, creation_date, physical_address, metadata, content_type 
			FROM gateway_multiparts
			WHERE upload_id = $1`,
			uploadID); err != nil {
			return nil, err
		}
		return &m, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(*MultipartUpload), nil
}

func (m *dbTracker) Delete(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return ErrInvalidUploadID
	}
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		res, err := tx.Exec(`DELETE FROM gateway_multiparts WHERE upload_id = $1`, uploadID)
		if err != nil {
			return nil, err
		}
		affected := res.RowsAffected()
		if affected != 1 {
			return nil, ErrMultipartUploadNotFound
		}
		return nil, nil
	})
	return err
}

func Migrate(ctx context.Context, d *pgxpool.Pool, _ blockparams.AdapterConfig, writer io.Writer) error {
	je := json.NewEncoder(writer)
	// Create header
	if err := je.Encode(kv.Header{
		LakeFSVersion:   version.Version,
		PackageName:     packageName,
		DBSchemaVersion: kv.InitialMigrateVersion,
		CreatedAt:       time.Now().UTC(),
	}); err != nil {
		return err
	}

	rows, err := d.Query(ctx, "SELECT * FROM gateway_multiparts")
	if err != nil {
		return err
	}
	defer rows.Close()
	rowScanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		m := MultipartUpload{}
		err = rowScanner.Scan(&m)
		if err != nil {
			return err
		}
		pr := protoFromMultipart(&m)
		value, err := proto.Marshal(pr)
		if err != nil {
			return err
		}
		key := []byte(m.UploadID)
		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(multipartsPartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return err
		}
	}

	return nil
}
