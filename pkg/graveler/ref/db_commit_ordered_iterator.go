package ref

import (
	"context"
	"errors"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type OrderedCommitIteratorOption func(oci *DBOrderedCommitIterator)

// WithOnlyAncestryLeaves causes the iterator to return only commits which are not the first parent of any other commit.
// Consider a commit graph where all non-first-parent edges are removed. This graph is a tree, and ancestry leaves are its leaves.
func WithOnlyAncestryLeaves() OrderedCommitIteratorOption {
	return func(oci *DBOrderedCommitIterator) {
		oci.onlyAncestryLeaves = true
	}
}

// NewDBOrderedCommitIterator returns an iterator over all commits in the given repository.
// Ordering is based on the Commit ID value.
func NewDBOrderedCommitIterator(ctx context.Context, database db.Database, repository *graveler.RepositoryRecord, prefetchSize int, opts ...OrderedCommitIteratorOption) (*DBOrderedCommitIterator, error) {
	res := &DBOrderedCommitIterator{
		ctx:          ctx,
		db:           database,
		repository:   repository,
		prefetchSize: prefetchSize,
		buf:          make([]*graveler.CommitRecord, 0, prefetchSize),
	}
	for _, opt := range opts {
		opt(res)
	}
	return res, nil
}

type DBOrderedCommitIterator struct {
	ctx                context.Context
	db                 db.Database
	repository         *graveler.RepositoryRecord
	prefetchSize       int
	buf                []*graveler.CommitRecord
	err                error
	value              *graveler.CommitRecord
	offset             string
	state              iteratorState
	onlyAncestryLeaves bool
}

func (iter *DBOrderedCommitIterator) Next() bool {
	if iter.err != nil {
		return false
	}
	iter.maybeFetch()

	// stage a value and increment offset
	if len(iter.buf) == 0 {
		return false
	}
	iter.value = iter.buf[0]
	iter.buf = iter.buf[1:]
	iter.offset = string(iter.value.CommitID)
	return true
}

func (iter *DBOrderedCommitIterator) maybeFetch() {
	if iter.state == iteratorStateDone {
		return
	}
	if len(iter.buf) > 0 {
		return
	}
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	q := psql.Select("id", "committer", "message", "creation_date", "meta_range_id", "parents", "metadata", "version", "generation").
		From("graveler_commits").
		Where(sq.Eq{"repository_id": iter.repository.RepositoryID})

	if iter.state == iteratorStateInit {
		iter.state = iteratorStateQuerying
		q = q.Where(sq.GtOrEq{"id": iter.offset})
	} else {
		q = q.Where(sq.Gt{"id": iter.offset})
	}

	var buf []*commitRecord

	if iter.onlyAncestryLeaves {
		notExistsCondition := psql.Select("*").
			Prefix("NOT EXISTS (").Suffix(")").
			From("graveler_commits c2").
			Where("c2.repository_id=graveler_commits.repository_id").
			Where("first_parent(c2.parents, c2.version)=graveler_commits.id")
		q = q.Where(notExistsCondition)
	}
	q = q.OrderBy("id ASC")
	q = q.Limit(uint64(iter.prefetchSize))
	query, args, err := q.ToSql()
	if err != nil {
		iter.err = err
		return
	}
	err = iter.db.Select(iter.ctx, &buf, query, args...)
	if err != nil {
		iter.err = err
		return
	}
	if len(buf) < iter.prefetchSize {
		iter.state = iteratorStateDone
	}
	for _, c := range buf {
		rec := c.toGravelerCommitRecord()
		iter.buf = append(iter.buf, rec)
	}
}

func (iter *DBOrderedCommitIterator) SeekGE(id graveler.CommitID) {
	if errors.Is(iter.err, ErrIteratorClosed) {
		return
	}
	iter.offset = string(id)
	iter.state = iteratorStateInit
	iter.buf = iter.buf[:0]
	iter.value = nil
	iter.err = nil
}

func (iter *DBOrderedCommitIterator) Value() *graveler.CommitRecord {
	if iter.err != nil {
		return nil
	}
	return iter.value
}

func (iter *DBOrderedCommitIterator) Err() error {
	return iter.err
}

func (iter *DBOrderedCommitIterator) Close() {
	iter.err = ErrIteratorClosed
	iter.buf = nil
}
