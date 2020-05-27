package catalog

import (
	"context"
	"time"

	"github.com/cloudfoundry/clock"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type EntryState int

const (
	EntryStateCommitted = iota
	EntryStateStage
	EntryStateUnstage

	ListRepoFieldID   = "id"
	ListRepoFieldName = "name"

	ListRepoDefaultField = ListRepoFieldID
	ListRepoDefaultLimit = 1000
)

type EntryReadOptions struct {
	EntryState EntryState
	CommitID   int
}

type ListReposOptions struct {
	Field string
	Limit int
	After interface{}
}

func WithListReposLimit(limit int) func(options *ListReposOptions) {
	return func(options *ListReposOptions) {
		options.Limit = limit
	}
}

func WithListReposBy(field string) func(options *ListReposOptions) {
	return func(options *ListReposOptions) {
		options.Field = field
	}
}

func WithListReposAfter(val interface{}) func(options *ListReposOptions) {
	return func(options *ListReposOptions) {
		options.After = val
	}
}

type Cataloger interface {
	WithContext(context.Context) Cataloger

	// repository level
	CreateRepo(name string, bucket string, branch string) (int, error)
	ListRepos(opts ...func(*ListReposOptions)) ([]*Repo, bool, error)
	GetRepo(repoID int) (*Repo, error)
	GetRepoByName(repoName string) (*Repo, error)
	DeleteRepo(repoID int) error
	GetRepoCommitLog(repoID int, fromCommitID string, results int, after string) ([]*Commit, bool, error)

	// branch level
	CreateBranch(repoID int, branch string, sourceBranchID int) (*Branch, error)
	GetBranch(branchID string) (*Branch, error)
	DeleteBranch(branchID int) error
	GetBranchCommitLog(branchID int, fromCommitID string, results int, after string) ([]*Commit, bool, error)
	ListBranchesByPrefix(repoID string, prefix string, amount int, after string) ([]*Branch, bool, error)
	Commit(branchID int, message, committer string, metadata map[string]string) (*Commit, error)

	// entry level
	ReadEntry(branchID int, path string, readOptions EntryReadOptions) (*Entry, error)
	WriteEntry(branchID int, path string, entry *Entry) error
	ListEntriesByPrefix(branchID int, path, after string, results int, readOptions EntryReadOptions, descend bool) ([]*Entry, bool, error)

	// diff and merge
	Diff(leftBranchID, rightBranchID int) (Differences, error)
	Merge(sourceBranchID, destinationBranchID int, userID string) (Differences, error)

	// revert
	RevertBranch(branchID int) error
	RevertCommit(branchID int, commit string) error
	RevertPath(branchID int, path string) error
	RevertEntry(branchID int, path string) error

	CreateDedupEntryIfNone(repoID int, dedupID string, physicalAddress string) (string, error)
	CreateMultiPartUpload(repoID int, path, physicalAddress string, creationTime time.Time) error
	ReadMultiPartUpload(repoID int, uploadID string) (*MultipartUpload, error)
	DeleteMultiPartUpload(repoID int, uploadID string) error
}

type cataloger struct {
	Clock clock.Clock
	ctx   context.Context
	log   logging.Logger
	db    db.Database
}

func NewCataloger(db db.Database) *cataloger {
	return &cataloger{
		Clock: clock.NewClock(),
		ctx:   context.Background(),
		log:   logging.Default().WithField("service_name", "cataloger"),
		db:    db,
	}
}

func (c *cataloger) WithContext(ctx context.Context) Cataloger {
	return &cataloger{
		Clock: c.Clock,
		ctx:   ctx,
		log:   logging.FromContext(ctx).WithField("service_name", "cataloger"),
		db:    c.db,
	}
}

func (c *cataloger) transactOpts(opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithContext(c.ctx),
		db.WithLogger(c.log),
	}
	for _, opt := range opts {
		o = append(o, opt)
	}
	return o
}

func (cataloger) GetRepo(repoID int) (*Repo, error) {
	panic("implement me")
}

func (cataloger) GetRepoByName(repoName string) (*Repo, error) {
	panic("implement me")
}

func (cataloger) DeleteRepo(repoID int) error {
	panic("implement me")
}

func (cataloger) GetRepoCommitLog(repoID int, fromCommitID string, results int, after string) ([]*Commit, bool, error) {
	panic("implement me")
}

func (cataloger) CreateBranch(repoID int, branch string, sourceBranchID int) (*Branch, error) {
	panic("implement me")
}

func (cataloger) GetBranch(branchID string) (*Branch, error) {
	panic("implement me")
}

func (cataloger) DeleteBranch(branchID int) error {
	panic("implement me")
}

func (cataloger) GetBranchCommitLog(branchID int, fromCommitID string, results int, after string) ([]*Commit, bool, error) {
	panic("implement me")
}

func (cataloger) ListBranchesByPrefix(repoID string, prefix string, amount int, after string) ([]*Branch, bool, error) {
	panic("implement me")
}

func (cataloger) Commit(branchID int, message, committer string, metadata map[string]string) (*Commit, error) {
	panic("implement me")
}

func (cataloger) ReadEntry(branchID int, path string, readOptions EntryReadOptions) (*Entry, error) {
	panic("implement me")
}

func (cataloger) WriteEntry(branchID int, path string, entry *Entry) error {
	panic("implement me")
}

func (cataloger) ListEntriesByPrefix(branchID int, path, after string, results int, readOptions EntryReadOptions, descend bool) ([]*Entry, bool, error) {
	panic("implement me")
}

func (cataloger) Diff(leftBranchID, rightBranchID int) (Differences, error) {
	panic("implement me")
}

func (cataloger) Merge(sourceBranchID, destinationBranchID int, userID string) (Differences, error) {
	panic("implement me")
}

func (cataloger) RevertBranch(branchID int) error {
	panic("implement me")
}

func (cataloger) RevertCommit(branchID int, commit string) error {
	panic("implement me")
}

func (cataloger) RevertPath(branchID int, path string) error {
	panic("implement me")
}

func (cataloger) RevertEntry(branchID int, path string) error {
	panic("implement me")
}

func (cataloger) CreateDedupEntryIfNone(repoID int, dedupID string, physicalAddress string) (string, error) {
	panic("implement me")
}

func (cataloger) CreateMultiPartUpload(repoID int, path, physicalAddress string, creationTime time.Time) error {
	panic("implement me")
}

func (cataloger) ReadMultiPartUpload(repoID int, uploadID string) (*MultipartUpload, error) {
	panic("implement me")
}

func (cataloger) DeleteMultiPartUpload(repoID int, uploadID string) error {
	panic("implement me")
}
