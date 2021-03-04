package ref

import (
	"context"
	"errors"

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
	state        iteratorState
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
	if ri.err != nil {
		return false
	}
	ri.maybeFetch()

	// stage a value and increment offset
	if len(ri.buf) == 0 {
		return false
	}
	ri.value = ri.buf[0]
	ri.buf = ri.buf[1:]
	ri.offset = string(ri.value.BranchID)
	return true
}

func (ri *BranchIterator) maybeFetch() {
	if ri.state == iteratorStateDone {
		return
	}
	if len(ri.buf) > 0 {
		return
	}

	var offsetCondition string
	if ri.state == iteratorStateInit {
		offsetCondition = iteratorOffsetCondition(true)
		ri.state = iteratorStateQuerying
	} else {
		offsetCondition = iteratorOffsetCondition(false)
	}

	var buf []*branchRecord
	err := ri.db.Select(ri.ctx, &buf, `
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
		ri.state = iteratorStateDone
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
	if errors.Is(ri.err, ErrIteratorClosed) {
		return
	}
	ri.offset = string(id)
	ri.state = iteratorStateInit
	ri.buf = ri.buf[:0]
	ri.value = nil
	ri.err = nil
}

func (ri *BranchIterator) Value() *graveler.BranchRecord {
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *BranchIterator) Err() error {
	return ri.err
}

func (ri *BranchIterator) Close() {
	ri.err = ErrIteratorClosed
	ri.buf = nil
}
