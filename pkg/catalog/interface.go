package catalog

import (
	"context"
	"io"

	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	DefaultPathDelimiter = "/"
)

type DiffParams struct {
	Limit            int
	After            string
	AdditionalFields []string // db fields names that will be load in additional to Path on Difference's Entry
}

type RevertParams struct {
	Reference    string // the commit to revert
	ParentNumber int    // if reverting a merge commit, the change will be reversed relative to this parent number (1-based).
	Committer    string
}

type ExpireResult struct {
	Repository        string
	Branch            string
	PhysicalAddress   string
	InternalReference string
}

// ExpiryRows is a database iterator over ExpiryResults.  Use Next to advance from row to row.
type ExpiryRows interface {
	Close()
	Next() bool
	Err() error
	// Read returns the current from ExpiryRows, or an error on failure.  Call it only after
	// successfully calling Next.
	Read() (*ExpireResult, error)
}

// GetEntryParams configures what entries GetEntry returns.
type GetEntryParams struct {
	// For entries to expired objects the Expired bit is set.  If true, GetEntry returns
	// successfully for expired entries, otherwise it returns the entry with ErrExpired.
	ReturnExpired bool
}

type Interface interface {
	// CreateRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with default branch name 'branch'
	CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) (*Repository, error)

	// CreateBareRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with no initial branch or commit
	// defaultBranchID will point to a non-existent branch on creation, it is up to the caller to eventually create it.
	CreateBareRepository(ctx context.Context, repository string, storageNamespace string, defaultBranchID string) (*Repository, error)

	// GetRepository get repository information
	GetRepository(ctx context.Context, repository string) (*Repository, error)

	// DeleteRepository delete a repository
	DeleteRepository(ctx context.Context, repository string) error

	// ListRepositories list repositories information, the bool returned is true when more repositories can be listed.
	// In this case pass the last repository name as 'after' on the next call to ListRepositories
	ListRepositories(ctx context.Context, limit int, after string) ([]*Repository, bool, error)

	CreateBranch(ctx context.Context, repository, branch string, sourceRef string) (*CommitLog, error)
	DeleteBranch(ctx context.Context, repository, branch string) error
	ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*Branch, bool, error)
	BranchExists(ctx context.Context, repository string, branch string) (bool, error)
	GetBranchReference(ctx context.Context, repository, branch string) (string, error)
	ResetBranch(ctx context.Context, repository, branch string) error

	CreateTag(ctx context.Context, repository, tagID string, ref string) (string, error)
	DeleteTag(ctx context.Context, repository, tagID string) error
	ListTags(ctx context.Context, repository string, limit int, after string) ([]*Tag, bool, error)
	GetTag(ctx context.Context, repository, tagID string) (string, error)

	// GetEntry returns the current entry for path in repository branch reference.  Returns
	// the entry with ExpiredError if it has expired from underlying storage.
	GetEntry(ctx context.Context, repository, reference string, path string, params GetEntryParams) (*DBEntry, error)
	CreateEntry(ctx context.Context, repository, branch string, entry DBEntry) error
	CreateEntries(ctx context.Context, repository, branch string, entries []DBEntry) error
	DeleteEntry(ctx context.Context, repository, branch string, path string) error
	ListEntries(ctx context.Context, repository, reference string, prefix, after string, delimiter string, limit int) ([]*DBEntry, bool, error)
	ResetEntry(ctx context.Context, repository, branch string, path string) error
	ResetEntries(ctx context.Context, repository, branch string, prefix string) error

	Commit(ctx context.Context, repository, branch string, message string, committer string, metadata Metadata) (*CommitLog, error)
	GetCommit(ctx context.Context, repository, reference string) (*CommitLog, error)
	ListCommits(ctx context.Context, repository, branch string, fromReference string, limit int) ([]*CommitLog, bool, error)

	// RollbackCommit sets the branch to point at the given commit, losing all later commits.
	RollbackCommit(ctx context.Context, repository, branch string, reference string) error
	// Revert creates a reverse patch to the given commit, and applies it as a new commit on the given branch.
	Revert(ctx context.Context, repository, branch string, params RevertParams) error

	Diff(ctx context.Context, repository, leftReference string, rightReference string, params DiffParams) (Differences, bool, error)
	Compare(ctx context.Context, repository, leftReference string, rightReference string, params DiffParams) (Differences, bool, error)
	DiffUncommitted(ctx context.Context, repository, branch string, limit int, after string) (Differences, bool, error)

	Merge(ctx context.Context, repository, destinationBranch, sourceRef, committer, message string, metadata Metadata) (*MergeResult, error)

	// dump/load metadata
	DumpCommits(ctx context.Context, repositoryID string) (string, error)
	DumpBranches(ctx context.Context, repositoryID string) (string, error)
	DumpTags(ctx context.Context, repositoryID string) (string, error)
	LoadCommits(ctx context.Context, repositoryID, commitsMetaRangeID string) error
	LoadBranches(ctx context.Context, repositoryID, branchesMetaRangeID string) error
	LoadTags(ctx context.Context, repositoryID, tagsMetaRangeID string) error

	// forward metadata for thick clients
	GetMetaRange(ctx context.Context, repositoryID, metaRangeID string) (graveler.MetaRangeInfo, error)
	GetRange(ctx context.Context, repositoryID, rangeID string) (graveler.RangeInfo, error)

	io.Closer
}
