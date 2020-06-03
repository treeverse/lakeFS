package catalog

import (
	"context"
	"time"

	"github.com/cloudfoundry/clock"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type Cataloger interface {
	// repository level
	CreateRepo(ctx context.Context, repo string, bucket string, branch string) error
	ListRepos(ctx context.Context, limit int, after string) ([]*Repo, bool, error)
	GetRepo(ctx context.Context, repo string) (*Repo, error)
	DeleteRepo(ctx context.Context, repo string) error
	ListRepoCommits(ctx context.Context, repo string, fromCommitID int, limit int) ([]*CommitLog, bool, error)

	// branch level
	CreateBranch(ctx context.Context, repo string, branch string, sourceBranch string) (*Branch, error)
	GetBranch(ctx context.Context, repo string, branch string) (*Branch, error)
	DeleteBranch(ctx context.Context, repo string, branch string) error
	ListBranchesByPrefix(ctx context.Context, repo string, prefix string, limit int, after string) ([]*Branch, bool, error)

	Commit(ctx context.Context, repo, branch, message, committer string, metadata map[string]string) (int, error)
	ListBranchCommits(ctx context.Context, repo string, branch string, fromCommitID int, limit int) ([]*CommitLog, bool, error)

	// entry level
	ReadEntry(ctx context.Context, repo, branch, path string, readUncommitted bool) (*Entry, error)
	WriteEntry(ctx context.Context, repo, branch, path, checksum, physicalAddress string, size int, metadata *map[string]string) error
	DeleteEntry(ctx context.Context, repo, branch, path string) error
	ListEntriesByPrefix(ctx context.Context, repo string, branch string, path, after string, limit int, descend bool, readUncommitted bool) ([]*Entry, bool, error)

	// diff and merge
	Diff(ctx context.Context, repo, leftBranch, rightBranch string) (Differences, error)
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

func (c *cataloger) DeleteEntry(ctx context.Context, repo string, branch string, path string) error {
	panic("implement me")
}
