package catalog

import (
	"context"
	"time"

	"github.com/cloudfoundry/clock"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type RepositoryCataloger interface {
	CreateRepository(ctx context.Context, repository string, bucket string, branch string) error
	GetRepository(ctx context.Context, repository string) (*Repository, error)
	DeleteRepository(ctx context.Context, repository string) error
	ListRepositories(ctx context.Context, limit int, after string) ([]*Repository, bool, error)
}

type BranchCataloger interface {
	CreateBranch(ctx context.Context, repository, branch string, sourceBranch string) (int, error)
	GetBranch(ctx context.Context, repository, branch string) (*Branch, error)
	DeleteBranch(ctx context.Context, repository, branch string) error
	ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*Branch, bool, error)
	ResetBranch(ctx context.Context, repository, branch string) error
}

type EntryCataloger interface {
	GetEntry(ctx context.Context, repository, reference string, path string) (*Entry, error)
	CreateEntry(ctx context.Context, repository, branch string, path, checksum, physicalAddress string, size int, metadata Metadata) error
	DeleteEntry(ctx context.Context, repository, branch string, path string) error
	ListEntries(ctx context.Context, repository, reference string, prefix, after string, limit int) ([]*Entry, bool, error)
	ResetEntry(ctx context.Context, repository, branch string, path string) error
	ResetEntries(ctx context.Context, repository, branch string, prefix string) error
}

type MultipartUpdateCataloger interface {
	CreateMultipartUpload(ctx context.Context, repository, uploadID, path, physicalAddress string, creationTime time.Time) error
	GetMultipartUpload(ctx context.Context, repository, uploadID string) (*MultipartUpload, error)
	DeleteMultipartUpload(ctx context.Context, repository, uploadID string) error
}

type Deduper interface {
	Dedup(ctx context.Context, repository string, dedupID string, physicalAddress string) (string, error)
}

type Committer interface {
	Commit(ctx context.Context, repository, branch string, message string, committer string, metadata Metadata) (string, error)
	ListCommits(ctx context.Context, repository, branch string, fromReference string, limit int) ([]*CommitLog, bool, error)
	RollbackCommit(ctx context.Context, repository, reference string) error
}

type Differ interface {
	Diff(ctx context.Context, repository, leftBranch string, rightBranch string) (Differences, error)
	DiffUncommitted(ctx context.Context, repository, branch string) (Differences, error)
}

type MergeResult struct {
	Differences Differences
	Reference   string
}

type Merger interface {
	Merge(ctx context.Context, repository, sourceBranch, destinationBranch string, committer string, metadata Metadata) (*MergeResult, error)
}

type Cataloger interface {
	RepositoryCataloger
	BranchCataloger
	EntryCataloger
	Committer
	MultipartUpdateCataloger
	Differ
	Merger
	Deduper
}

// cataloger main catalog implementation based on mvcc
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

func (c *cataloger) RollbackCommit(ctx context.Context, repository, reference string) error {
	panic("implement me")
}
