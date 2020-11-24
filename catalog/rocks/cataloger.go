package rocks

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/catalog"
)

type cataloger struct{}

func NewCataloger() catalog.Cataloger {
	return &cataloger{}
}

// CreateRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with default branch name 'branch'
func (c *cataloger) CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) (*catalog.Repository, error) {
	panic("not implemented") // TODO: Implement
}

// GetRepository get repository information
func (c *cataloger) GetRepository(ctx context.Context, repository string) (*catalog.Repository, error) {
	panic("not implemented") // TODO: Implement
}

// DeleteRepository delete a repository
func (c *cataloger) DeleteRepository(ctx context.Context, repository string) error {
	panic("not implemented") // TODO: Implement
}

// ListRepositories list repositories information, the bool returned is true when more repositories can be listed.
// In this case pass the last repository name as 'after' on the next call to ListRepositories
func (c *cataloger) ListRepositories(ctx context.Context, limit int, after string) ([]*catalog.Repository, bool, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) CreateBranch(ctx context.Context, repository string, branch string, sourceBranch string) (*catalog.CommitLog, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) DeleteBranch(ctx context.Context, repository string, branch string) error {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*catalog.Branch, bool, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) BranchExists(ctx context.Context, repository string, branch string) (bool, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) GetBranchReference(ctx context.Context, repository string, branch string) (string, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) ResetBranch(ctx context.Context, repository string, branch string) error {
	panic("not implemented") // TODO: Implement
}

// GetEntry returns the current entry for path in repository branch reference.  Returns
// the entry with ExpiredError if it has expired from underlying storage.
func (c *cataloger) GetEntry(ctx context.Context, repository string, reference string, path string, params catalog.GetEntryParams) (*catalog.Entry, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) CreateEntry(ctx context.Context, repository string, branch string, entry catalog.Entry, params catalog.CreateEntryParams) error {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) CreateEntries(ctx context.Context, repository string, branch string, entries []catalog.Entry) error {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) DeleteEntry(ctx context.Context, repository string, branch string, path string) error {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) ListEntries(ctx context.Context, repository string, reference string, prefix string, after string, delimiter string, limit int) ([]*catalog.Entry, bool, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) ResetEntry(ctx context.Context, repository string, branch string, path string) error {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) ResetEntries(ctx context.Context, repository string, branch string, prefix string) error {
	panic("not implemented") // TODO: Implement
}

// QueryEntriesToExpire returns ExpiryRows iterating over all objects to expire on
// repositoryName according to policy.
func (c *cataloger) QueryEntriesToExpire(ctx context.Context, repositoryName string, policy *catalog.Policy) (catalog.ExpiryRows, error) {
	panic("not implemented") // TODO: Implement
}

// MarkEntriesExpired marks all entries identified by expire as expired.  It is a batch operation.
func (c *cataloger) MarkEntriesExpired(ctx context.Context, repositoryName string, expireResults []*catalog.ExpireResult) error {
	panic("not implemented") // TODO: Implement
}

// MarkObjectsForDeletion marks objects in catalog_object_dedup as "deleting" if all
// their entries are expired, and returns the new total number of objects marked (or an
// error).  These objects are not yet safe to delete: there could be a race between
// marking objects as expired deduping newly-uploaded objects.  See
// DeleteOrUnmarkObjectsForDeletion for that actual deletion.
func (c *cataloger) MarkObjectsForDeletion(ctx context.Context, repositoryName string) (int64, error) {
	panic("not implemented") // TODO: Implement
}

// DeleteOrUnmarkObjectsForDeletion scans objects in catalog_object_dedup for objects
// marked "deleting" and returns an iterator over physical addresses of those objects
// all of whose referring entries are still expired.  If called after MarkEntriesExpired
// and MarkObjectsForDeletion this is safe, because no further entries can refer to
// expired objects.  It also removes the "deleting" mark from those objects that have an
// entry _not_ marked as expiring and therefore were not on the returned rows.
func (c *cataloger) DeleteOrUnmarkObjectsForDeletion(ctx context.Context, repositoryName string) (catalog.StringIterator, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) DedupReportChannel() chan *catalog.DedupReport {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) CreateMultipartUpload(ctx context.Context, repository string, uploadID string, path string, physicalAddress string, creationTime time.Time) error {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) GetMultipartUpload(ctx context.Context, repository string, uploadID string) (*catalog.MultipartUpload, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) DeleteMultipartUpload(ctx context.Context, repository string, uploadID string) error {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) Commit(ctx context.Context, repository string, branch string, message string, committer string, metadata catalog.Metadata) (*catalog.CommitLog, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) GetCommit(ctx context.Context, repository string, reference string) (*catalog.CommitLog, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) ListCommits(ctx context.Context, repository string, branch string, fromReference string, limit int) ([]*catalog.CommitLog, bool, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) RollbackCommit(ctx context.Context, repository string, reference string) error {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) Diff(ctx context.Context, repository string, leftReference string, rightReference string, params catalog.DiffParams) (catalog.Differences, bool, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) DiffUncommitted(ctx context.Context, repository string, branch string, limit int, after string) (catalog.Differences, bool, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) Merge(ctx context.Context, repository string, leftBranch string, rightBranch string, committer string, message string, metadata catalog.Metadata) (*catalog.MergeResult, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) Hooks() *catalog.CatalogerHooks {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) GetExportConfigurationForBranch(repository string, branch string) (catalog.ExportConfiguration, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) GetExportConfigurations() ([]catalog.ExportConfigurationForBranch, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) PutExportConfiguration(repository string, branch string, conf *catalog.ExportConfiguration) error {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) ExportStateSet(repo string, branch string, cb catalog.ExportStateCallback) error {
	panic("not implemented") // TODO: Implement
}

// GetExportState returns the current Export state params
func (c *cataloger) GetExportState(repo string, branch string) (catalog.ExportState, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) Close() error {
	panic("not implemented") // TODO: Implement
}
