package ref

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type BranchIterator struct {
	db  db.Database
	ctx context.Context

	repositoryID graveler.RepositoryID
	value        *graveler.BranchRecord
	buf          []*graveler.BranchRecord

	// blockValue is true when the iterator is created, or SeekGE() is called.
	// A single Next() call turns it to false. When it's true, Value() returns nil.
	blockValue bool

	offset      string
	fetchSize   int
	shouldFetch bool

	err error
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
		shouldFetch:  true,
		blockValue:   true,
	}
}

func (ri *BranchIterator) Next() bool {
	ri.blockValue = false

	// no buffer is initialized
	if ri.buf == nil {
		ri.fetch(true) // initial fetch
	} else if len(ri.buf) == 0 {
		ri.fetch(false) // paging size we're out of values
	}

	if len(ri.buf) == 0 {
		return false
	}

	// stage a value and increment offset
	ri.value = ri.buf[0]
	ri.offset = string(ri.value.BranchID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = make([]*graveler.BranchRecord, 0)
	}

	return true
}

func (ri *BranchIterator) fetch(initial bool) {
	if !ri.shouldFetch {
		return
	}
	offsetCondition := iteratorOffsetCondition(initial)
	buf := make([]*branchRecord, 0)
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
		ri.shouldFetch = false
	}
	ri.buf = make([]*graveler.BranchRecord, len(buf))
	for i, b := range buf {
		ri.buf[i] = &graveler.BranchRecord{
			BranchID: b.BranchID,
			Branch: &graveler.Branch{
				CommitID:     b.CommitID,
				StagingToken: b.StagingToken,
			},
		}
	}
}

func (ri *BranchIterator) SeekGE(id graveler.BranchID) {
	ri.offset = string(id)
	ri.shouldFetch = true
	ri.buf = nil
	ri.blockValue = true
}

func (ri *BranchIterator) Value() *graveler.BranchRecord {
	if ri.blockValue || ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *BranchIterator) Err() error {
	return ri.err
}

func (ri *BranchIterator) Close() {}
