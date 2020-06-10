package catalog

import (
	"context"
	"time"

	"github.com/cloudfoundry/clock"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type Cataloger interface {
	// repository
	CreateRepository(ctx context.Context, repository string, bucket string, branch string) error
	GetRepository(ctx context.Context, repository string) (*Repo, error)
	DeleteRepository(ctx context.Context, repository string) error
	ListRepositories(ctx context.Context, limit int, after string) ([]*Repo, bool, error)

	// branch
	CreateBranch(ctx context.Context, repository string, branch string, sourceBranch string) (int, error)
	GetBranch(ctx context.Context, repository string, branch string) (*Branch, error)
	DeleteBranch(ctx context.Context, repository string, branch string) error
	ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*Branch, bool, error)
	RevertBranch(ctx context.Context, repository string, branch string) error

	// commit
	Commit(ctx context.Context, repository string, branch string, message string, committer string, metadata Metadata) (int, error)
	ListCommits(ctx context.Context, repository string, branch string, fromCommitID int, limit int) ([]*CommitLog, bool, error)
	RevertCommit(ctx context.Context, repository string, branch string, commitID int) error

	// entry
	GetEntry(ctx context.Context, repository string, branch string, path string, readUncommitted bool) (*Entry, error)
	CreateEntry(ctx context.Context, repository string, branch string, path, checksum, physicalAddress string, size int, metadata Metadata) error
	DeleteEntry(ctx context.Context, repository string, branch string, path string) error
	ListEntries(ctx context.Context, repository string, branch string, path string, after string, limit int, readUncommitted bool) ([]*Entry, bool, error)
	RevertEntry(ctx context.Context, repository string, branch string, path string) error
	RevertEntries(ctx context.Context, repository string, branch string, prefix string) error

	// diff and merge
	Diff(ctx context.Context, repository, leftBranch string, rightBranch string) (Differences, error)
	Merge(ctx context.Context, sourceBranch, destinationBranch string, userID string) (Differences, error)

	// dedup
	Dedup(ctx context.Context, repository string, dedupID string, physicalAddress string) (string, error)

	// multipart
	CreateMultipartUpload(ctx context.Context, repository, uploadID, path, physicalAddress string, creationTime time.Time) error
	GetMultipartUpload(ctx context.Context, repository, uploadID string) (*MultipartUpload, error)
	DeleteMultipartUpload(ctx context.Context, repository, uploadID string) error
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

func (c *cataloger) RevertCommit(ctx context.Context, repository string, branch string, commitID int) error {
	panic("implement me")
}
