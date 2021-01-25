package migrate

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/db"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Iterator struct {
	rows  pgx.Rows
	value *rocks.EntryRecord
	err   error
}

var ErrNotSeekable = errors.New("iterator isn't seekable")

// NewIterator returns an iterator over an mvcc branch/commit, giving entries as EntryCatalog.
func NewIterator(ctx context.Context, db db.Database, branchID int64, commitID int64) (*Iterator, error) {
	db = db.WithContext(ctx)
	// query for list entries
	query, args, err := mvcc.ListEntriesQuery(db, branchID, mvcc.CommitID(commitID), "", "", -1)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Iterator{
		rows: rows,
	}, nil
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}

	if !it.rows.Next() {
		it.err = it.rows.Err()
		it.value = nil
		return false
	}

	var ent rocks.Entry
	var entPath string
	var entTS time.Time
	it.err = it.rows.Scan(&entPath, &ent.Address, &entTS, &ent.Size, &ent.ETag, &ent.Metadata)
	if it.err != nil {
		it.value = nil
		return false
	}
	ent.LastModified = timestamppb.New(entTS)
	it.value = &rocks.EntryRecord{
		Path:  rocks.Path(entPath),
		Entry: &ent,
	}
	return true
}

func (it *Iterator) SeekGE(rocks.Path) {
	if it.err != nil {
		return
	}
	it.rows.Close()
	it.rows = nil
	it.err = ErrNotSeekable
}

func (it *Iterator) Value() *rocks.EntryRecord {
	if it.err != nil {
		return nil
	}
	return it.value
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() {
	if it.rows == nil {
		return
	}
	it.rows.Close()
	it.rows = nil
	it.value = nil
}

type emptyIterator struct{}

func (e *emptyIterator) Next() bool {
	return false
}

func (e *emptyIterator) SeekGE(rocks.Path) {
}

func (e *emptyIterator) Value() *rocks.EntryRecord {
	return nil
}

func (e *emptyIterator) Err() error {
	return nil
}

func (e *emptyIterator) Close() {}

func newEmptyIterator() rocks.EntryIterator {
	return &emptyIterator{}
}
