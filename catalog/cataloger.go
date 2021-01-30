package catalog

import (
	"context"
	"io"

	"github.com/lib/pq"
)

const (
	DefaultCommitter        = ""
	DefaultBranchName       = "master"
	DefaultImportBranchName = "import-from-inventory"
	DefaultPathDelimiter    = "/"
)

type DiffParams struct {
	Limit            int
	After            string
	AdditionalFields []string // db fields names that will be load in additional to Path on Difference's Entry
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

type Cataloger interface {
	// CreateRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with default branch name 'branch'
	CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) (*Repository, error)

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
	GetEntry(ctx context.Context, repository, reference string, path string, params GetEntryParams) (*Entry, error)
	CreateEntry(ctx context.Context, repository, branch string, entry Entry) error
	CreateEntries(ctx context.Context, repository, branch string, entries []Entry) error
	DeleteEntry(ctx context.Context, repository, branch string, path string) error
	ListEntries(ctx context.Context, repository, reference string, prefix, after string, delimiter string, limit int) ([]*Entry, bool, error)
	ResetEntry(ctx context.Context, repository, branch string, path string) error
	ResetEntries(ctx context.Context, repository, branch string, prefix string) error

	Commit(ctx context.Context, repository, branch string, message string, committer string, metadata Metadata) (*CommitLog, error)
	GetCommit(ctx context.Context, repository, reference string) (*CommitLog, error)
	ListCommits(ctx context.Context, repository, branch string, fromReference string, limit int) ([]*CommitLog, bool, error)

	// RollbackCommit sets the branch to point at the given commit, losing all later commits.
	RollbackCommit(ctx context.Context, repository, branch string, reference string) error
	// Revert creates a reverse patch to the commit given as 'reference', and applies it as a new commit on the given branch.
	Revert(ctx context.Context, repository, branch string, reference string, committer string) error

	Diff(ctx context.Context, repository, leftReference string, rightReference string, params DiffParams) (Differences, bool, error)
	Compare(ctx context.Context, repository, leftReference string, rightReference string, params DiffParams) (Differences, bool, error)
	DiffUncommitted(ctx context.Context, repository, branch string, limit int, after string) (Differences, bool, error)

	Merge(ctx context.Context, repository, leftBranch, rightBranch, committer, message string, metadata Metadata) (*MergeResult, error)

	Hooks() *CatalogerHooks

	GetExportConfigurationForBranch(repository string, branch string) (ExportConfiguration, error)
	GetExportConfigurations() ([]ExportConfigurationForBranch, error)
	PutExportConfiguration(repository string, branch string, conf *ExportConfiguration) error

	ExportStateSet(repo, branch string, cb ExportStateCallback) error
	// GetExportState returns the current Export state params
	GetExportState(repo string, branch string) (ExportState, error)

	io.Closer
}

// ExportStateCallback returns the new ref, state and message regarding the old ref and state
type ExportStateCallback func(oldRef string, state CatalogBranchExportStatus) (newRef string, newState CatalogBranchExportStatus, newMessage *string, err error)

// ExportConfiguration describes the export configuration of a branch, as passed on wire, used
// internally, and stored in DB.
type ExportConfiguration struct {
	Path                   string         `db:"export_path" json:"export_path"`
	StatusPath             string         `db:"export_status_path" json:"export_status_path"`
	LastKeysInPrefixRegexp pq.StringArray `db:"last_keys_in_prefix_regexp" json:"last_keys_in_prefix_regexp"`
	IsContinuous           bool           `db:"continuous" json:"is_continuous"`
}

// ExportConfigurationForBranch describes how to export BranchID.  It is stored in the database.
// Unfortunately golang sql doesn't know about embedded structs, so you get a useless copy of
// ExportConfiguration embedded here.
type ExportConfigurationForBranch struct {
	Repository string `db:"repository"`
	Branch     string `db:"branch"`

	Path                   string         `db:"export_path"`
	StatusPath             string         `db:"export_status_path"`
	LastKeysInPrefixRegexp pq.StringArray `db:"last_keys_in_prefix_regexp"`
	IsContinuous           bool           `db:"continuous"`
}

type PostCommitFunc func(ctx context.Context, repo, branch string, commitLog CommitLog) error
type PostMergeFunc func(ctx context.Context, repo, branch string, mergeResult MergeResult) error

// CatalogerHooks describes the hooks available for some operations on the catalog.  Hooks are
// called after the transaction ends; if they return an error they do not affect commit/merge.
type CatalogerHooks struct {
	// PostCommit hooks are called at the end of a commit.
	PostCommit []PostCommitFunc

	// PostMerge hooks are called at the end of a merge.
	PostMerge []PostMergeFunc
}

func (h *CatalogerHooks) AddPostCommit(f PostCommitFunc) *CatalogerHooks {
	h.PostCommit = append(h.PostCommit, f)
	return h
}

func (h *CatalogerHooks) AddPostMerge(f PostMergeFunc) *CatalogerHooks {
	h.PostMerge = append(h.PostMerge, f)
	return h
}
