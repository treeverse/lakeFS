package ref

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type CommitIterator struct {
	db           db.Database
	ctx          context.Context
	repositoryID graveler.RepositoryID
	value        *graveler.CommitRecord
	next         graveler.CommitID
	err          error
}

func NewCommitIterator(ctx context.Context, db db.Database, repositoryID graveler.RepositoryID, start graveler.CommitID) *CommitIterator {
	return &CommitIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		next:         start,
	}
}

func (ci *CommitIterator) Next() bool {
	if ci.value == nil {
		return ci.fetch()
	}
	if len(ci.value.Commit.Parents) > 0 {
		ci.next = ci.value.Commit.Parents[0]
		return ci.fetch()
	}
	return false
}

func (ci *CommitIterator) fetch() bool {
	if ci.next == "" {
		return false
	}
	var rec commitRecord
	err := ci.db.WithContext(ci.ctx).Get(&rec, `
		SELECT id, committer, message, creation_date, parents, tree_id, metadata
		FROM graveler_commits
		WHERE repository_id = $1 AND id = $2`,
		ci.repositoryID, ci.next)
	if err != nil {
		ci.err = err
		return false
	}
	ci.value = rec.toGravelerCommitRecord()
	return true
}

func (ci *CommitIterator) SeekGE(id graveler.CommitID) {
	// Setting value to nil so that next Value() call
	// returns nil as the interface commands
	ci.value = nil
	ci.next = id
}

func (ci *CommitIterator) Value() *graveler.CommitRecord {
	if ci.err != nil {
		return nil
	}
	return ci.value
}

func (ci *CommitIterator) Err() error {
	return ci.err
}

func (ci *CommitIterator) Close() {}
