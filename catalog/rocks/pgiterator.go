package rocks

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

const (
	// IteratorPrefetchSize is the amount of records to prefetch from PG
	IteratorPrefetchSize = 1000
)

type pgBranchRecord struct {
	BranchID `db:"id"`
	*PgBranch
}

type PGRepositoryIterator struct {
	db  db.Database
	ctx context.Context

	value *RepositoryRecord
	buf   []*RepositoryRecord

	offset           string
	prefetchSize     int
	prefetchRequired bool

	err error
}

func NewRepositoryIterator(ctx context.Context, db db.Database, prefetchSize int, offset string) *PGRepositoryIterator {
	return &PGRepositoryIterator{
		db:               db,
		ctx:              ctx,
		prefetchSize:     prefetchSize,
		prefetchRequired: true,
		offset:           offset,
	}
}

func (ri *PGRepositoryIterator) Next() bool {
	// no buffer is initialized
	if ri.buf == nil || len(ri.buf) == 0 {
		ri.prefetch()
	}

	if len(ri.buf) == 0 {
		return false
	}

	// stage a value and increment offset
	ri.value = ri.buf[0]
	ri.offset = string(ri.value.RepositoryID)
	if len(ri.buf) >= 2 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = make([]*RepositoryRecord, 0)
	}

	return true

}

func (ri *PGRepositoryIterator) prefetch() bool {
	if !ri.prefetchRequired {
		return false
	}
	err := ri.db.WithContext(ri.ctx).Select(&ri.buf, `
			SELECT id, storage_namespace, creation_date, default_branch
			FROM kv_repositories
			WHERE id > $1
			ORDER BY id ASC
			LIMIT $2`, ri.offset, ri.prefetchSize)
	if err != nil {
		ri.err = err
		return false
	}
	if len(ri.buf) < ri.prefetchSize {
		ri.prefetchRequired = false
	}
	return len(ri.buf) > 0
}

func (ri *PGRepositoryIterator) SeekGE(id RepositoryID) bool {
	ri.offset = string(id)
	ri.prefetchRequired = true
	ri.buf = make([]*RepositoryRecord, 0)
	ri.prefetch()

	if len(ri.buf) == 0 {
		return false
	}

	// stage a value and increment offset
	ri.value = ri.buf[len(ri.buf)-1]
	ri.offset = string(ri.value.RepositoryID)
	ri.buf = ri.buf[0 : len(ri.buf)-1]
	return true
}

func (ri *PGRepositoryIterator) Value() *RepositoryRecord {
	return ri.value
}

func (ri *PGRepositoryIterator) Err() error {
	return ri.err
}

func (ri *PGRepositoryIterator) Close() {

}

type PGBranchIterator struct {
	db  db.Database
	ctx context.Context

	repositoryID RepositoryID
	value        *BranchRecord
	buf          []*BranchRecord

	offset           string
	prefetchSize     int
	prefetchRequired bool

	err error
}

func NewBranchIterator(ctx context.Context, db db.Database, repositoryID RepositoryID, prefetchSize int, offset string) *PGBranchIterator {
	return &PGBranchIterator{
		db:               db,
		ctx:              ctx,
		repositoryID:     repositoryID,
		prefetchSize:     prefetchSize,
		prefetchRequired: true,
		offset:           offset,
	}
}

func (ri *PGBranchIterator) Next() bool {
	// no buffer is initialized
	if ri.buf == nil || len(ri.buf) == 0 {
		ri.prefetch()
	}

	if len(ri.buf) == 0 {
		return false
	}

	// stage a value and increment offset
	ri.value = ri.buf[0]
	ri.offset = string(ri.value.BranchID)
	if len(ri.buf) >= 2 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = make([]*BranchRecord, 0)
	}

	return true
}

func (ri *PGBranchIterator) prefetch() bool {
	if !ri.prefetchRequired {
		return false
	}
	buf := make([]*pgBranchRecord, 0)
	err := ri.db.WithContext(ri.ctx).Select(&buf, `
			SELECT id, staging_token, commit_id
			FROM kv_branches
			WHERE repository_id = $1
			AND id > $2
			ORDER BY id ASC
			LIMIT $3`, ri.repositoryID, ri.offset, ri.prefetchSize)
	if err != nil {
		ri.err = err
		return false
	}
	if len(buf) < ri.prefetchSize {
		ri.prefetchRequired = false
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
	return len(ri.buf) > 0
}

func (ri *PGBranchIterator) SeekGE(id BranchID) bool {
	ri.offset = string(id)
	ri.prefetchRequired = true
	ri.buf = make([]*BranchRecord, 0)
	ri.prefetch()

	if len(ri.buf) == 0 {
		return false
	}

	// stage a value and increment offset
	ri.value = ri.buf[len(ri.buf)-1]
	ri.offset = string(ri.value.BranchID)
	ri.buf = ri.buf[0 : len(ri.buf)-1]
	return true
}

func (ri *PGBranchIterator) Value() *BranchRecord {
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
		FROM kv_commits
		WHERE repository_id = $1 AND id = $2	
	`, ci.repositoryID, ci.next)
	if err != nil {
		ci.err = err
		return false
	}
	ci.value = record
	return true
}

func (ci *PGCommitIterator) SeekGE(id CommitID) bool {
	ci.next = id
	return ci.fetch()
}

func (ci *PGCommitIterator) Value() *CommitRecord {
	return ci.value
}

func (ci *PGCommitIterator) Err() error {
	return ci.err
}

func (ci *PGCommitIterator) Close() {

}
