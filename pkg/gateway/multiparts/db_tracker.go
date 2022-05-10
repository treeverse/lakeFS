package multiparts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/export"
	"google.golang.org/protobuf/proto"
)

// TODO(niro): Remove after Migration version
//nolint:gochecknoinits
func init() {
	db.KVRegister("multiparts", Migrate)
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

func Migrate(d db.Database, writer io.Writer) error {
	ctx := context.Background()
	je := json.NewEncoder(writer)

	// Create header
	err := je.Encode(export.Header{
		Version:   kv.MigrateVersion + 1,
		Timestamp: time.Now(),
	})
	if err != nil {
		return err
	}

	entries := make([]MultipartUpload, 0)
	err = d.Select(ctx, &entries, "SELECT * FROM gateway_multiparts")
	if err != nil {
		return err
	}
	for i := range entries {
		pr := protoFromMultipart(&entries[i]) // gosec error WA
		value, err := proto.Marshal(pr)
		key := []byte(kv.FormatPath(multipartPrefix, entries[i].UploadID))
		if err != nil {
			return err
		}
		err = je.Encode(export.Entry{
			Key:   key,
			Value: value,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
