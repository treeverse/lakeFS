package graveler

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

const (
	// IteratorPrefetchSize is the amount of records to fetch from PG
	IteratorPrefetchSize = 1000

	iteratorOffsetGE = ">="
	iteratorOffsetGT = ">"
)

type pgBranchRecord struct {
	BranchID `db:"id"`
	*PgBranch
}

type pgTagRecord struct {
	TagID    `db:"id"`
	CommitID `db:"commit_id"`
}

type PGRepositoryIterator struct {
	db  db.Database
	ctx context.Context

	value *RepositoryRecord
	buf   []*RepositoryRecord

	offset      string
	fetchSize   int
	shouldFetch bool

	// blockValue is true when the iterator is created, or SeekGE() is called.
	// A single Next() call turns it to false. When it's true, Value() returns nil.
	blockValue bool

	err error
}

func NewRepositoryIterator(ctx context.Context, db db.Database, fetchSize int, offset string) *PGRepositoryIterator {
	return &PGRepositoryIterator{
		db:          db,
		ctx:         ctx,
		fetchSize:   fetchSize,
		shouldFetch: true,
		offset:      offset,
		blockValue:  true,
	}
}

func (ri *PGRepositoryIterator) Next() bool {
	ri.blockValue = false

	// no buffer is initialized
	if ri.buf == nil {
		ri.fetch(true) // initial fetch
	} else if len(ri.buf) == 0 {
		ri.fetch(false) // paginating since we're out of values
	}

	if len(ri.buf) == 0 {
		return false
	}

	// stage a value and increment offset
	ri.value = ri.buf[0]
	ri.offset = string(ri.value.RepositoryID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = make([]*RepositoryRecord, 0)
	}

	return true
}

func iteratorOffsetCondition(initial bool) string {
	if initial {
		return iteratorOffsetGE
	}
	return iteratorOffsetGT
}

func (ri *PGRepositoryIterator) fetch(initial bool) {
	if !ri.shouldFetch {
		return
	}
	offsetCondition := iteratorOffsetCondition(initial)
	ri.err = ri.db.WithContext(ri.ctx).Select(&ri.buf, `
			SELECT id, storage_namespace, creation_date, default_branch
			FROM graveler_repositories
			WHERE id `+offsetCondition+` $1
			ORDER BY id ASC
			LIMIT $2`, ri.offset, ri.fetchSize)
	if ri.err != nil {
		return
	}
	if len(ri.buf) < ri.fetchSize {
		ri.shouldFetch = false
	}
}

func (ri *PGRepositoryIterator) SeekGE(id RepositoryID) {
	ri.offset = string(id)
	ri.shouldFetch = true
	ri.buf = nil
	ri.blockValue = true
}

func (ri *PGRepositoryIterator) Value() *RepositoryRecord {
	if ri.blockValue || ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *PGRepositoryIterator) Err() error {
	return ri.err
}

func (ri *PGRepositoryIterator) Close() {}

type PGBranchIterator struct {
	db  db.Database
	ctx context.Context

	repositoryID RepositoryID
	value        *BranchRecord
	buf          []*BranchRecord

	// blockValue is true when the iterator is created, or SeekGE() is called.
	// A single Next() call turns it to false. When it's true, Value() returns nil.
	blockValue bool

	offset      string
	fetchSize   int
	shouldFetch bool

	err error
}

func NewBranchIterator(ctx context.Context, db db.Database, repositoryID RepositoryID, prefetchSize int, offset string) *PGBranchIterator {
	return &PGBranchIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		fetchSize:    prefetchSize,
		shouldFetch:  true,
		blockValue:   true,
		offset:       offset,
	}
}

func (ri *PGBranchIterator) Next() bool {
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
		ri.buf = make([]*BranchRecord, 0)
	}

	return true
}

func (ri *PGBranchIterator) fetch(initial bool) {
	if !ri.shouldFetch {
		return
	}
	offsetCondition := iteratorOffsetCondition(initial)
	buf := make([]*pgBranchRecord, 0)
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
	ri.buf = make([]*BranchRecord, len(buf))
	for i, b := range buf {
		ri.buf[i] = &BranchRecord{
			BranchID: b.BranchID,
			Branch: &Branch{
				CommitID:     b.CommitID,
				stagingToken: b.StagingToken,
			},
		}
	}
}

func (ri *PGBranchIterator) SeekGE(id BranchID) {
	ri.offset = string(id)
	ri.shouldFetch = true
	ri.buf = nil
	ri.blockValue = true
}

func (ri *PGBranchIterator) Value() *BranchRecord {
	if ri.blockValue || ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *PGBranchIterator) Err() error {
	return ri.err
}

func (ri *PGBranchIterator) Close() {

}

type PGCommitIterator struct {
	db           db.Database
	ctx          context.Context
	repositoryID RepositoryID

	value *CommitRecord
	next  CommitID

	err error
}

func NewCommitIterator(ctx context.Context, db db.Database, repositoryID RepositoryID, start CommitID) *PGCommitIterator {
	return &PGCommitIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		next:         start,
	}
}

func (ci *PGCommitIterator) Next() bool {
	if ci.value == nil {
		return ci.fetch()
	}
	if len(ci.value.Commit.Parents) > 0 {
		ci.next = ci.value.Commit.Parents[0]
		return ci.fetch()
	}
	return false
}

func (ci *PGCommitIterator) fetch() bool {
	if ci.next == "" {
		return false
	}
	record := &CommitRecord{}
	err := ci.db.WithContext(ci.ctx).Get(record, `
		SELECT id, committer, message, creation_date, parents, tree_id, metadata
		FROM graveler_commits
		WHERE repository_id = $1 AND id = $2	
	`, ci.repositoryID, ci.next)
	if err != nil {
		ci.err = err
		return false
	}
	ci.value = record
	return true
}

func (ci *PGCommitIterator) SeekGE(id CommitID) {
	// Setting value to nil so that next Value() call
	// returns nil as the interface commands
	ci.value = nil
	ci.next = id
}

func (ci *PGCommitIterator) Value() *CommitRecord {
	if ci.err != nil {
		return nil
	}
	return ci.value
}

func (ci *PGCommitIterator) Err() error {
	return ci.err
}

func (ci *PGCommitIterator) Close() {}

type PGTagIterator struct {
	db           db.Database
	ctx          context.Context
	repositoryID RepositoryID
	value        *TagRecord
	buf          []*TagRecord
	// blockValue is true when the iterator is created, or SeekGE() is called.
	// A single Next() call turns it to false. When it's true, Value() returns nil.
	blockValue  bool
	offset      string
	fetchSize   int
	shouldFetch bool
	err         error
}

func NewTagIterator(ctx context.Context, db db.Database, repositoryID RepositoryID, prefetchSize int, offset string) *PGTagIterator {
	return &PGTagIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		fetchSize:    prefetchSize,
		shouldFetch:  true,
		blockValue:   true,
		offset:       offset,
	}
}

func (ri *PGTagIterator) Next() bool {
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
	ri.offset = string(ri.value.CommitID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = ri.buf[:0]
	}
	return true
}

func (ri *PGTagIterator) fetch(initial bool) {
	if !ri.shouldFetch {
		return
	}
	offsetCondition := iteratorOffsetCondition(initial)
	buf := make([]*pgTagRecord, 0)
	err := ri.db.WithContext(ri.ctx).Select(&buf, `
			SELECT id, commit_id
			FROM graveler_tags
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
	ri.buf = make([]*TagRecord, len(buf))
	for i, b := range buf {
		ri.buf[i] = &TagRecord{
			TagID:    b.TagID,
			CommitID: b.CommitID,
		}
	}
}

func (ri *PGTagIterator) SeekGE(id TagID) {
	ri.offset = string(id)
	ri.shouldFetch = true
	ri.buf = nil
	ri.blockValue = true
}

func (ri *PGTagIterator) Value() *TagRecord {
	if ri.blockValue || ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *PGTagIterator) Err() error {
	return ri.err
}

func (ri *PGTagIterator) Close() {}
