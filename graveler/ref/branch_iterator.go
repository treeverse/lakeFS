package ref

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type BranchIterator struct {
	db           db.Database
	ctx          context.Context
	repositoryID graveler.RepositoryID
	value        *graveler.BranchRecord
	buf          []*graveler.BranchRecord
	offset       string
	fetchSize    int
	err          error
	fetchCalled  bool
	fetchEnded   bool
	closed       bool
}

type branchRecord struct {
	BranchID     graveler.BranchID     `db:"id"`
	CommitID     graveler.CommitID     `db:"commit_id"`
	StagingToken graveler.StagingToken `db:"staging_token"`
}

func NewBranchIterator(ctx context.Context, db db.Database, repositoryID graveler.RepositoryID, prefetchSize int) *BranchIterator {
	return &BranchIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		fetchSize:    prefetchSize,
		buf:          make([]*graveler.BranchRecord, 0, prefetchSize),
	}
}

func (ri *BranchIterator) Next() bool {
	if ri.closed {
		panic(ErrIteratorClosed)
	}
	if ri.err != nil {
		return false
	}
	ri.fetch()

	// stage a value and increment offset
	if len(ri.buf) == 0 {
		return false
	}
	ri.value = ri.buf[0]
	ri.offset = string(ri.value.BranchID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = ri.buf[:0]
	}
	return true
}

func (ri *BranchIterator) fetch() {
	if ri.fetchEnded {
		return
	}
	if len(ri.buf) > 0 {
		return
	}
	offsetCondition := iteratorOffsetCondition(!ri.fetchCalled)
	ri.fetchCalled = true

	var buf []*branchRecord
	err := ri.db.WithContext(ri.ctx).Select(&buf, `
			SELECT id, staging_token, commit_id
			FROM graveler_branches
			WHERE repository_id = $1
			AND id `+offsetCondition+` $2
			ORDER BY id ASC
			LIMIT $3`, ri.repositoryID, ri.offset, ri.fetchSize)
	if err != nil {
		ri.err = err
		return
	}
	if len(buf) < ri.fetchSize {
		ri.fetchEnded = true
	}
	for _, b := range buf {
		rec := &graveler.BranchRecord{
			BranchID: b.BranchID,
			Branch: &graveler.Branch{
				CommitID:     b.CommitID,
				StagingToken: b.StagingToken,
			},
		}
		ri.buf = append(ri.buf, rec)
	}
}

func (ri *BranchIterator) SeekGE(id graveler.BranchID) {
	if ri.closed {
		panic(ErrIteratorClosed)
	}
	ri.offset = string(id)
	ri.fetchEnded = false
	ri.fetchCalled = false
	ri.buf = nil
	ri.value = nil
	ri.err = nil
}

func (ri *BranchIterator) Value() *graveler.BranchRecord {
	if ri.closed {
		panic(ErrIteratorClosed)
	}
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *BranchIterator) Err() error {
	if ri.closed {
		panic(ErrIteratorClosed)
	}
	return ri.err
}

func (ri *BranchIterator) Close() {
	ri.closed = true
}
