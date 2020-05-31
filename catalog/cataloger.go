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
)

type EntryReadOptions struct {
	EntryState EntryState
	CommitID   int
}

type Cataloger interface {
	// repository level
	CreateRepo(ctx context.Context, repo string, bucket string, branch string) error
	ListRepos(ctx context.Context, limit int, after string) ([]*Repo, bool, error)
	GetRepo(ctx context.Context, repo string) (*Repo, error)
	DeleteRepo(ctx context.Context, repo string) error
	GetRepoCommitLog(ctx context.Context, repo string, fromCommitID int, results int, after int) ([]*Commit, bool, error)

	// branch level
	CreateBranch(ctx context.Context, repo string, branch string, sourceBranch string) (*Branch, error)
	GetBranch(ctx context.Context, repo string, branch string) (*Branch, error)
	DeleteBranch(ctx context.Context, branch string) error
	GetBranchCommitLog(ctx context.Context, branch string, fromCommitID int, results int, after int) ([]*Commit, bool, error)
	ListBranchesByPrefix(ctx context.Context, repo string, prefix string, amount int, after string) ([]*Branch, bool, error)
	Commit(ctx context.Context, branch string, message, committer string, metadata map[string]string) (*Commit, error)

	// entry level
	ReadEntry(ctx context.Context, branchID int, path string, readOptions EntryReadOptions) (*Entry, error)
	WriteEntry(ctx context.Context, branchID int, path string, entry *Entry) error
	ListEntriesByPrefix(ctx context.Context, branchID int, path, after string, results int, readOptions EntryReadOptions, descend bool) ([]*Entry, bool, error)

	// diff and merge
	Diff(ctx context.Context, leftBranch, rightBranch string) (Differences, error)
	Merge(ctx context.Context, sourceBranch, destinationBranch string, userID string) (Differences, error)

	// revert
	RevertBranch(ctx context.Context, branch string) error
	RevertCommit(ctx context.Context, branch string, commitID int) error
	RevertPath(ctx context.Context, branch string, path string) error
	RevertEntry(ctx context.Context, branch string, path string) error

	// dedup
	GetOrCreateDedup(ctx context.Context, repo string, dedupID string, physicalAddress string) (string, error)

	// multipart
	CreateMultipartUpload(ctx context.Context, repo, uploadID, path, physicalAddress string, creationTime time.Time) error
	ReadMultipartUpload(ctx context.Context, repo, uploadID string) (*MultipartUpload, error)
	DeleteMultipartUpload(ctx context.Context, repo, uploadID string) error
}

type cataloger struct {
	Clock clock.Clock
	log   logging.Logger
	db    db.Database
}

func NewCataloger(db db.Database) Cataloger {
	return &cataloger{
		Clock: clock.NewClock(),
		log:   logging.Default().WithField("service_name", "cataloger"),
		db:    db,
	}
}

func (c *cataloger) txOpts(ctx context.Context, opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithContext(ctx),
		db.WithLogger(c.log),
	}
	return append(o, opts...)
}

func (c *cataloger) GetRepoCommitLog(ctx context.Context, repo string, fromCommitID int, results int, after int) ([]*Commit, bool, error) {
	panic("implement me")
}

func (c *cataloger) DeleteBranch(ctx context.Context, branch string) error {
	panic("implement me")
}

func (c *cataloger) GetBranchCommitLog(ctx context.Context, branch string, fromCommitID int, results int, after int) ([]*Commit, bool, error) {
	panic("implement me")
}

func (c *cataloger) ListBranchesByPrefix(ctx context.Context, repo string, prefix string, amount int, after string) ([]*Branch, bool, error) {
	panic("implement me")
}

func (c *cataloger) Commit(ctx context.Context, branch string, message, committer string, metadata map[string]string) (*Commit, error) {
	panic("implement me")
}

func (c *cataloger) ReadEntry(ctx context.Context, branchID int, path string, readOptions EntryReadOptions) (*Entry, error) {
	panic("implement me")
}

//func (c *cataloger) WriteEntry(ctx context.Context, branchID int, path string, entry *Entry) error {
//	panic("implement me")
//}

func (c *cataloger) ListEntriesByPrefix(ctx context.Context, branchID int, path, after string, results int, readOptions EntryReadOptions, descend bool) ([]*Entry, bool, error) {
	panic("implement me")
}

func (c *cataloger) Diff(ctx context.Context, leftBranch, rightBranch string) (Differences, error) {
	panic("implement me")
}

func (c *cataloger) Merge(ctx context.Context, sourceBranch, destinationBranch string, userID string) (Differences, error) {
	panic("implement me")
}

func (c *cataloger) RevertBranch(ctx context.Context, branch string) error {
	panic("implement me")
}

func (c *cataloger) RevertCommit(ctx context.Context, branch string, commitID int) error {
	panic("implement me")
}

func (c *cataloger) RevertPath(ctx context.Context, branch string, path string) error {
	panic("implement me")
}

func (c *cataloger) RevertEntry(ctx context.Context, branch string, path string) error {
	panic("implement me")
}
