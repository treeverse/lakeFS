package ref

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type DBBranchIterator struct {
	db              db.Database
	ctx             context.Context
	repositoryID    graveler.RepositoryID
	value           *graveler.BranchRecord
	buf             []*graveler.BranchRecord
	offset          string
	fetchSize       int
	err             error
	state           iteratorState
	orderByCommitID bool
}

type BranchIteratorOption func(bi *DBBranchIterator)

func WithOrderByCommitID() BranchIteratorOption {
	return func(bi *DBBranchIterator) {
		bi.orderByCommitID = true
	}
}

type branchRecord struct {
	BranchID     graveler.BranchID     `db:"id"`
	CommitID     graveler.CommitID     `db:"commit_id"`
	StagingToken graveler.StagingToken `db:"staging_token"`
}

func NewDBBranchIterator(ctx context.Context, db db.Database, repositoryID graveler.RepositoryID, prefetchSize int, opts ...BranchIteratorOption) *DBBranchIterator {
	res := &DBBranchIterator{
		db:              db,
		ctx:             ctx,
		repositoryID:    repositoryID,
		fetchSize:       prefetchSize,
		buf:             make([]*graveler.BranchRecord, 0, prefetchSize),
		orderByCommitID: false,
	}
	for _, opt := range opts {
		opt(res)
	}
	return res
}

func (ri *DBBranchIterator) Next() bool {
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

func (ri *DBBranchIterator) maybeFetch() {
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
			ORDER BY `+ri.getOrderBy()+`  ASC
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

func (ri *DBBranchIterator) SeekGE(id graveler.BranchID) {
	if errors.Is(ri.err, ErrIteratorClosed) {
		return
	}
	ri.offset = id.String()
	ri.state = iteratorStateInit
	ri.buf = ri.buf[:0]
	ri.value = nil
	ri.err = nil
}

func (ri *DBBranchIterator) Value() *graveler.BranchRecord {
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *DBBranchIterator) Err() error {
	return ri.err
}

func (ri *DBBranchIterator) Close() {
	ri.err = ErrIteratorClosed
	ri.buf = nil
}

func (ri *DBBranchIterator) getOrderBy() string {
	if ri.orderByCommitID {
		return "commit_id"
	}
	return "id"
}
