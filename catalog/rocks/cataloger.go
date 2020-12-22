package rocks

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
)

type cataloger struct {
	EntryCatalog EntryCatalog
	log          logging.Logger
	dummyDedupCh chan *catalog.DedupReport
	hooks        catalog.CatalogerHooks
}

const (
	ListRepositoriesLimitMax = 1000
	ListBranchesLimitMax     = 1000
	ListEntriesLimitMax      = 10000
)

func NewCataloger() catalog.Cataloger {
	return &cataloger{
		EntryCatalog: NewEntryCatalog(),
		log:          logging.Default(),
		dummyDedupCh: make(chan *catalog.DedupReport),
		hooks:        catalog.CatalogerHooks{},
	}
}

// CreateRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with default branch name 'branch'
func (c *cataloger) CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) (*catalog.Repository, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return nil, err
	}
	storageNS, err := graveler.NewStorageNamespace(storageNamespace)
	if err != nil {
		return nil, err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return nil, err
	}
	repo, err := c.EntryCatalog.CreateRepository(ctx, repositoryID, storageNS, branchID)
	if err != nil {
		return nil, err
	}
	catalogRepo := &catalog.Repository{
		Name:             repositoryID.String(),
		StorageNamespace: storageNS.String(),
		DefaultBranch:    branchID.String(),
		CreationDate:     repo.CreationDate,
	}
	return catalogRepo, nil
}

// GetRepository get repository information
func (c *cataloger) GetRepository(ctx context.Context, repository string) (*catalog.Repository, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return nil, err
	}
	repo, err := c.EntryCatalog.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	catalogRepository := &catalog.Repository{
		Name:             repositoryID.String(),
		StorageNamespace: repo.StorageNamespace.String(),
		DefaultBranch:    repo.DefaultBranchID.String(),
		CreationDate:     repo.CreationDate,
	}
	return catalogRepository, nil
}

// DeleteRepository delete a repository
func (c *cataloger) DeleteRepository(ctx context.Context, repository string) error {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return err
	}
	return c.EntryCatalog.DeleteRepository(ctx, repositoryID)
}

// ListRepositories list repositories information, the bool returned is true when more repositories can be listed.
// In this case pass the last repository name as 'after' on the next call to ListRepositories
func (c *cataloger) ListRepositories(ctx context.Context, limit int, after string) ([]*catalog.Repository, bool, error) {
	// normalize limit
	if limit < 0 || limit > ListRepositoriesLimitMax {
		limit = ListRepositoriesLimitMax
	}
	// get list repositories iterator
	it, err := c.EntryCatalog.ListRepositories(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("get iterator: %w", err)
	}
	// seek for first item
	repositoryID, err := graveler.NewRepositoryID(after)
	if err != nil {
		return nil, false, fmt.Errorf("after as repository id: %w", err)
	}
	it.SeekGE(repositoryID)

	var repos []*catalog.Repository
	for it.Next() {
		record := it.Value()
		repos = append(repos, &catalog.Repository{
			Name:             record.RepositoryID.String(),
			StorageNamespace: record.StorageNamespace.String(),
			DefaultBranch:    record.DefaultBranchID.String(),
			CreationDate:     record.CreationDate,
		})
		// collect limit +1 to return limit and has more
		if len(repos) >= limit+1 {
			break
		}
	}
	if it.Err() != nil {
		return nil, false, it.Err()
	}
	// trim result if needed and return has more
	hasMore := false
	if len(repos) >= limit {
		hasMore = true
		repos = repos[:limit]
	}
	return repos, hasMore, nil
}

func (c *cataloger) CreateBranch(ctx context.Context, repository string, branch string, sourceBranch string) (*catalog.CommitLog, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return nil, err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return nil, err
	}
	sourceRef, err := graveler.NewRef(sourceBranch)
	if err != nil {
		return nil, err
	}
	newBranch, err := c.EntryCatalog.CreateBranch(ctx, repositoryID, branchID, sourceRef)
	if err != nil {
		return nil, err
	}
	commit, err := c.EntryCatalog.GetCommit(ctx, repositoryID, newBranch.CommitID)
	if err != nil {
		return nil, err
	}
	catalogCommitLog := &catalog.CommitLog{
		Reference: newBranch.CommitID.String(),
		Committer: commit.Committer,
		Message:   commit.Message,
		Metadata:  catalog.Metadata(commit.Metadata),
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, string(parent))
	}
	return catalogCommitLog, nil
}

func (c *cataloger) DeleteBranch(ctx context.Context, repository string, branch string) error {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return err
	}
	return c.EntryCatalog.DeleteBranch(ctx, repositoryID, branchID)
}

func (c *cataloger) ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*catalog.Branch, bool, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return nil, false, err
	}
	prefixBranch, err := graveler.NewBranchID(prefix)
	if err != nil {
		return nil, false, err
	}
	afterBranch, err := graveler.NewBranchID(after)
	if err != nil {
		return nil, false, err
	}
	// normalize limit
	if limit < 0 || limit > ListBranchesLimitMax {
		limit = ListBranchesLimitMax
	}
	it, err := c.EntryCatalog.ListBranches(ctx, repositoryID)
	if err != nil {
		return nil, false, err
	}
	if afterBranch == "" {
		it.SeekGE(prefixBranch)
	} else {
		it.SeekGE(afterBranch)
	}
	var branches []*catalog.Branch
	for it.Next() {
		v := it.Value()
		branchID := v.BranchID.String()
		// break in case we got to a branch outside our prefix
		if !strings.HasPrefix(branchID, prefix) {
			break
		}
		branch := &catalog.Branch{
			Repository: repositoryID.String(),
			Name:       v.BranchID.String(),
		}
		branches = append(branches, branch)
		if len(branches) >= limit+1 {
			break
		}
	}
	if it.Err() != nil {
		return nil, false, it.Err()
	}
	// return results (optional trimmed) and hasMore
	hasMore := false
	if len(branches) >= limit {
		hasMore = true
		branches = branches[:limit]
	}
	return branches, hasMore, nil
}

func (c *cataloger) BranchExists(ctx context.Context, repository string, branch string) (bool, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return false, err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return false, err
	}
	_, err = c.EntryCatalog.GetBranch(ctx, repositoryID, branchID)
	return err != nil, err
}

func (c *cataloger) GetBranchReference(ctx context.Context, repository string, branch string) (string, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return "", err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return "", err
	}
	b, err := c.EntryCatalog.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return "", err
	}
	return string(b.CommitID), nil
}

func (c *cataloger) ResetBranch(ctx context.Context, repository string, branch string) error {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return err
	}
	return c.EntryCatalog.Reset(ctx, repositoryID, branchID)
}

// GetEntry returns the current entry for path in repository branch reference.  Returns
// the entry with ExpiredError if it has expired from underlying storage.
func (c *cataloger) GetEntry(ctx context.Context, repository string, reference string, path string, _ catalog.GetEntryParams) (*catalog.Entry, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return nil, err
	}
	ref, err := graveler.NewRef(reference)
	if err != nil {
		return nil, err
	}
	p, err := NewPath(path)
	if err != nil {
		return nil, err
	}
	ent, err := c.EntryCatalog.GetEntry(ctx, repositoryID, ref, p)
	if err != nil {
		return nil, err
	}
	catalogEntry := &catalog.Entry{
		Path:            p.String(),
		PhysicalAddress: ent.Address,
		CreationDate:    ent.LastModified.AsTime(),
		Size:            ent.Size,
		Checksum:        hex.EncodeToString(ent.ETag),
		Metadata:        ent.Metadata,
	}
	return catalogEntry, nil
}

func (c *cataloger) CreateEntry(ctx context.Context, repository string, branch string, entry catalog.Entry, _ catalog.CreateEntryParams) error {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return err
	}
	p, err := NewPath(entry.Path)
	if err != nil {
		return err
	}
	etag, err := hex.DecodeString(entry.Checksum)
	if err != nil {
		return err
	}
	ent := &Entry{
		Address:  entry.PhysicalAddress,
		Metadata: map[string]string(entry.Metadata),
		ETag:     etag,
		Size:     entry.Size,
	}
	return c.EntryCatalog.SetEntry(ctx, repositoryID, branchID, p, ent)
}

func (c *cataloger) CreateEntries(ctx context.Context, repository string, branch string, entries []catalog.Entry) error {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		p, err := NewPath(entry.Path)
		if err != nil {
			return err
		}
		etag, err := hex.DecodeString(entry.Checksum)
		if err != nil {
			return err
		}
		ent := &Entry{
			Address:  entry.PhysicalAddress,
			Metadata: map[string]string(entry.Metadata),
			ETag:     etag,
			Size:     entry.Size,
		}
		if err := c.EntryCatalog.SetEntry(ctx, repositoryID, branchID, p, ent); err != nil {
			return err
		}
	}
	return nil
}

func (c *cataloger) DeleteEntry(ctx context.Context, repository string, branch string, path string) error {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return err
	}
	p, err := NewPath(path)
	if err != nil {
		return err
	}
	return c.EntryCatalog.DeleteEntry(ctx, repositoryID, branchID, p)
}

func (c *cataloger) ListEntries(ctx context.Context, repository string, reference string, prefix string, after string, delimiter string, limit int) ([]*catalog.Entry, bool, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return nil, false, err
	}
	ref, err := graveler.NewRef(reference)
	if err != nil {
		return nil, false, err
	}
	prefixPath, err := NewPath(prefix)
	if err != nil {
		return nil, false, err
	}
	delimiterPath, err := NewPath(delimiter)
	if err != nil {
		return nil, false, err
	}
	afterPath, err := NewPath(after)
	if err != nil {
		return nil, false, err
	}
	it, err := c.EntryCatalog.ListEntries(ctx, repositoryID, ref, prefixPath, delimiterPath)
	if err != nil {
		return nil, false, err
	}
	it.SeekGE(afterPath)
	// normalize limit
	if limit < 0 || limit > ListEntriesLimitMax {
		limit = ListEntriesLimitMax
	}
	var entries []*catalog.Entry
	for it.Next() {
		v := it.Value()
		entry := &catalog.Entry{
			CommonLevel: v.CommonPrefix,
			Path:        v.Path.String(),
		}
		if v.Entry != nil {
			entry.PhysicalAddress = v.Address
			entry.CreationDate = v.LastModified.AsTime()
			entry.Size = v.Size
			entry.Checksum = hex.EncodeToString(v.ETag)
			entry.Metadata = v.Metadata
			entry.Expired = false
		}
		entries = append(entries, entry)
		if len(entries) >= limit+1 {
			break
		}
	}
	if it.Err() != nil {
		return nil, false, it.Err()
	}
	// trim result if needed and return has more
	hasMore := false
	if len(entries) >= limit {
		hasMore = true
		entries = entries[:limit]
	}
	return entries, hasMore, nil
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
	return c.dummyDedupCh
}

func (c *cataloger) Commit(ctx context.Context, repository string, branch string, message string, committer string, metadata catalog.Metadata) (*catalog.CommitLog, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return nil, err
	}
	branchID, err := graveler.NewBranchID(branch)
	if err != nil {
		return nil, err
	}
	commitID, err := c.EntryCatalog.Commit(ctx, repositoryID, branchID, committer, message, map[string]string(metadata))
	if err != nil {
		return nil, err
	}
	catalogCommitLog := &catalog.CommitLog{
		Reference: commitID.String(),
		Committer: committer,
		Message:   message,
		Metadata:  metadata,
	}
	// in order to return commit log we need the commit creation time and parents
	commit, err := c.EntryCatalog.GetCommit(ctx, repositoryID, commitID)
	if err != nil {
		return catalogCommitLog, graveler.ErrCommitNotFound
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, parent.String())
	}
	catalogCommitLog.CreationDate = commit.CreationDate
	return catalogCommitLog, nil
}

func (c *cataloger) GetCommit(ctx context.Context, repository string, reference string) (*catalog.CommitLog, error) {
	repositoryID, err := graveler.NewRepositoryID(repository)
	if err != nil {
		return nil, err
	}
	ref, err := graveler.NewRef(reference)
	if err != nil {
		return nil, err
	}
	commitID, err := c.EntryCatalog.Dereference(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	commit, err := c.EntryCatalog.GetCommit(ctx, repositoryID, commitID)
	if err != nil {
		return nil, err
	}
	catalogCommitLog := &catalog.CommitLog{
		Reference:    ref.String(),
		Committer:    commit.Committer,
		Message:      commit.Message,
		CreationDate: commit.CreationDate,
		Metadata:     catalog.Metadata(commit.Metadata),
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, string(parent))
	}
	return catalogCommitLog, nil
}

func (c *cataloger) ListCommits(ctx context.Context, repository string, branch string, fromReference string, limit int) ([]*catalog.CommitLog, bool, error) {
	panic("not implemented") // TODO: Implement
}

func (c *cataloger) RollbackCommit(ctx context.Context, repository string, branch string, reference string) error {
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
	return &c.hooks
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
	close(c.dummyDedupCh)
	return nil
}
