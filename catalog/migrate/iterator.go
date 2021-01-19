package migrate

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/db"
)

type iteratorState int

const (
	iteratorStateInit iteratorState = iota
	iteratorStateQuerying
	iteratorStateDone
)

type Iterator struct {
	state     iteratorState
	ctx       context.Context
	db        db.Database
	branchID  int64
	commitID  int64
	buf       []*rocks.EntryRecord
	err       error
	offset    string
	fetchSize int
	value     *rocks.EntryRecord
}

var ErrIteratorClosed = errors.New("iterator closed")

// NewIterator returns an iterator over an mvcc branch/commit, giving entries as EntryCatalog.
func NewIterator(ctx context.Context, db db.Database, branchID int64, commitID int64, fetchSize int) *Iterator {
	return &Iterator{
		ctx:       ctx,
		db:        db,
		branchID:  branchID,
		commitID:  commitID,
		buf:       make([]*rocks.EntryRecord, 0, fetchSize),
		fetchSize: fetchSize,
	}
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}

	it.maybeFetch()

	// stage a value and increment offset
	if len(it.buf) == 0 {
		return false
	}
	it.value = it.buf[0]
	it.buf = it.buf[1:]
	it.offset = string(it.value.Path)
	return true
}

func (it *Iterator) SeekGE(id rocks.Path) {
	if errors.Is(it.err, ErrIteratorClosed) {
		return
	}
	it.state = iteratorStateInit
	it.offset = id.String()
	it.buf = it.buf[:0]
	it.value = nil
	it.err = nil
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
	it.buf = nil
	it.state = iteratorStateDone
	it.err = ErrIteratorClosed
}

func (it *Iterator) maybeFetch() {
	if it.state == iteratorStateDone {
		return
	}
	if len(it.buf) > 0 {
		return
	}
	if it.state == iteratorStateInit {
		it.state = iteratorStateQuerying
	}

	var res interface{}
	res, it.err = it.db.Transact(func(tx db.Tx) (interface{}, error) {
		return mvcc.ListEntriesTx(tx, it.branchID, mvcc.CommitID(it.commitID), "", it.offset, it.fetchSize)
	}, db.WithContext(it.ctx), db.ReadOnly())
	if it.err != nil {
		return
	}
	entries := res.([]*catalog.Entry)
	for _, entry := range entries {
		rec := &rocks.EntryRecord{
			Path:  rocks.Path(entry.Path),
			Entry: rocks.EntryFromCatalogEntry(*entry),
		}
		it.buf = append(it.buf, rec)
	}
	if len(it.buf) < it.fetchSize {
		it.state = iteratorStateDone
	} else if len(it.buf) > 0 {
		it.offset = it.buf[len(it.buf)-1].Path.String()
	}
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

func (e *emptyIterator) Close() {
}

func newEmptyIterator() rocks.EntryIterator {
	return &emptyIterator{}
}
