package catalog

import (
	"context"
	"io"
	"time"

	"github.com/lib/pq"
)

const (
	DefaultCommitter        = ""
	DefaultBranchName       = "master"
	DefaultImportBranchName = "import-from-inventory"
	DefaultPathDelimiter    = "/"
)

type DedupReport struct {
	Repository         string
	StorageNamespace   string
	DedupID            string
	Entry              *Entry
	NewPhysicalAddress string
	Timestamp          time.Time
}

type DedupParams struct {
	ID               string
	StorageNamespace string
}

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

type CreateEntryParams struct {
	Dedup DedupParams
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

	CreateBranch(ctx context.Context, repository, branch string, sourceBranch string) (*CommitLog, error)
	DeleteBranch(ctx context.Context, repository, branch string) error
	ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*Branch, bool, error)
	BranchExists(ctx context.Context, repository string, branch string) (bool, error)
	GetBranchReference(ctx context.Context, repository, branch string) (string, error)
	ResetBranch(ctx context.Context, repository, branch string) error

	// GetEntry returns the current entry for path in repository branch reference.  Returns
	// the entry with ExpiredError if it has expired from underlying storage.
	GetEntry(ctx context.Context, repository, reference string, path string, params GetEntryParams) (*Entry, error)
	CreateEntry(ctx context.Context, repository, branch string, entry Entry, params CreateEntryParams) error
	CreateEntries(ctx context.Context, repository, branch string, entries []Entry) error
	DeleteEntry(ctx context.Context, repository, branch string, path string) error
	ListEntries(ctx context.Context, repository, reference string, prefix, after string, delimiter string, limit int) ([]*Entry, bool, error)
	ResetEntry(ctx context.Context, repository, branch string, path string) error
	ResetEntries(ctx context.Context, repository, branch string, prefix string) error

	// QueryEntriesToExpire returns ExpiryRows iterating over all objects to expire on
	// repositoryName according to policy.
	QueryEntriesToExpire(ctx context.Context, repositoryName string, policy *Policy) (ExpiryRows, error)
	// MarkEntriesExpired marks all entries identified by expire as expired.  It is a batch operation.
	MarkEntriesExpired(ctx context.Context, repositoryName string, expireResults []*ExpireResult) error
	// MarkObjectsForDeletion marks objects in catalog_object_dedup as "deleting" if all
	// their entries are expired, and returns the new total number of objects marked (or an
	// error).  These objects are not yet safe to delete: there could be a race between
	// marking objects as expired deduping newly-uploaded objects.  See
	// DeleteOrUnmarkObjectsForDeletion for that actual deletion.
	MarkObjectsForDeletion(ctx context.Context, repositoryName string) (int64, error)
	// DeleteOrUnmarkObjectsForDeletion scans objects in catalog_object_dedup for objects
	// marked "deleting" and returns an iterator over physical addresses of those objects
	// all of whose referring entries are still expired.  If called after MarkEntriesExpired
	// and MarkObjectsForDeletion this is safe, because no further entries can refer to
	// expired objects.  It also removes the "deleting" mark from those objects that have an
	// entry _not_ marked as expiring and therefore were not on the returned rows.
	DeleteOrUnmarkObjectsForDeletion(ctx context.Context, repositoryName string) (StringIterator, error)

	DedupReportChannel() chan *DedupReport

	Commit(ctx context.Context, repository, branch string, message string, committer string, metadata Metadata) (*CommitLog, error)
	GetCommit(ctx context.Context, repository, reference string) (*CommitLog, error)
	ListCommits(ctx context.Context, repository, branch string, fromReference string, limit int) ([]*CommitLog, bool, error)
	RollbackCommit(ctx context.Context, repository, branch string, reference string) error

	Diff(ctx context.Context, repository, leftReference string, rightReference string, params DiffParams) (Differences, bool, error)
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
