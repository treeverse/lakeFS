package rocks

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type cataloger struct {
	EntryCatalog  *EntryCatalog
	log           logging.Logger
	dummyDedupeCh chan *catalog.DedupReport
	hooks         catalog.CatalogerHooks
}

const (
	ListRepositoriesLimitMax = 1000
	ListBranchesLimitMax     = 1000
	ListTagsLimitMax         = 1000
	DiffLimitMax             = 1000
	ListEntriesLimitMax      = 10000
)

var ErrUnknownDiffType = errors.New("unknown graveler difference type")

func NewCataloger(db db.Database, cfg *config.Config) (catalog.Cataloger, error) {
	entryCatalog, err := NewEntryCatalog(cfg, db)
	if err != nil {
		return nil, err
	}
	return &cataloger{
		EntryCatalog:  entryCatalog,
		log:           logging.Default(),
		dummyDedupeCh: make(chan *catalog.DedupReport),
		hooks:         catalog.CatalogerHooks{},
	}, nil
}

// CreateRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with default branch name 'branch'
func (c *cataloger) CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) (*catalog.Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	storageNS := graveler.StorageNamespace(storageNamespace)
	branchID := graveler.BranchID(branch)
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
	repositoryID := graveler.RepositoryID(repository)
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
	repositoryID := graveler.RepositoryID(repository)
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
	afterRepositoryID := graveler.RepositoryID(after)
	if afterRepositoryID != "" {
		it.SeekGE(afterRepositoryID)
	}

	var repos []*catalog.Repository
	for it.Next() {
		record := it.Value()
		if record.RepositoryID == afterRepositoryID {
			continue
		}
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
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	// trim result if needed and return has more
	hasMore := false
	if len(repos) > limit {
		hasMore = true
		repos = repos[:limit]
	}
	return repos, hasMore, nil
}

func (c *cataloger) CreateBranch(ctx context.Context, repository string, branch string, sourceBranch string) (*catalog.CommitLog, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	sourceRef := graveler.Ref(sourceBranch)
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
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	return c.EntryCatalog.DeleteBranch(ctx, repositoryID, branchID)
}

func (c *cataloger) ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*catalog.Branch, bool, error) {
	// normalize limit
	if limit < 0 || limit > ListBranchesLimitMax {
		limit = ListBranchesLimitMax
	}
	it, err := c.EntryCatalog.ListBranches(ctx, graveler.RepositoryID(repository))
	if err != nil {
		return nil, false, err
	}
	afterBranch := graveler.BranchID(after)
	prefixBranch := graveler.BranchID(prefix)
	if afterBranch < prefixBranch {
		it.SeekGE(prefixBranch)
	} else {
		it.SeekGE(afterBranch)
	}
	var branches []*catalog.Branch
	for it.Next() {
		v := it.Value()
		if v.BranchID == afterBranch {
			continue
		}
		branchID := v.BranchID.String()
		// break in case we got to a branch outside our prefix
		if !strings.HasPrefix(branchID, prefix) {
			break
		}
		branch := &catalog.Branch{
			Name:      v.BranchID.String(),
			Reference: v.CommitID.String(),
		}
		branches = append(branches, branch)
		if len(branches) >= limit+1 {
			break
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	// return results (optional trimmed) and hasMore
	hasMore := false
	if len(branches) > limit {
		hasMore = true
		branches = branches[:limit]
	}
	return branches, hasMore, nil
}

func (c *cataloger) BranchExists(ctx context.Context, repository string, branch string) (bool, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	_, err := c.EntryCatalog.GetBranch(ctx, repositoryID, branchID)
	if errors.Is(err, graveler.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *cataloger) GetBranchReference(ctx context.Context, repository string, branch string) (string, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	b, err := c.EntryCatalog.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return "", err
	}
	return string(b.CommitID), nil
}

func (c *cataloger) ResetBranch(ctx context.Context, repository string, branch string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	return c.EntryCatalog.Reset(ctx, repositoryID, branchID)
}

func (c *cataloger) CreateTag(ctx context.Context, repository string, tagID string, ref string) (string, error) {
	repositoryID := graveler.RepositoryID(repository)
	tag := graveler.TagID(tagID)
	commitID, err := c.EntryCatalog.Dereference(ctx, repositoryID, graveler.Ref(ref))
	if err != nil {
		return "", err
	}
	err = c.EntryCatalog.CreateTag(ctx, repositoryID, tag, commitID)
	if err != nil {
		return "", err
	}
	return commitID.String(), nil
}

func (c *cataloger) DeleteTag(ctx context.Context, repository string, tagID string) error {
	repositoryID := graveler.RepositoryID(repository)
	tag := graveler.TagID(tagID)
	return c.EntryCatalog.DeleteTag(ctx, repositoryID, tag)
}

func (c *cataloger) ListTags(ctx context.Context, repository string, limit int, after string) ([]*catalog.Tag, bool, error) {
	if limit < 0 || limit > ListTagsLimitMax {
		limit = ListTagsLimitMax
	}
	repositoryID := graveler.RepositoryID(repository)
	it, err := c.EntryCatalog.ListTags(ctx, repositoryID)
	if err != nil {
		return nil, false, err
	}
	afterTagID := graveler.TagID(after)
	it.SeekGE(afterTagID)

	var tags []*catalog.Tag
	for it.Next() {
		v := it.Value()
		if v.TagID == afterTagID {
			continue
		}
		branch := &catalog.Tag{
			ID:       string(v.TagID),
			CommitID: v.CommitID.String(),
		}
		tags = append(tags, branch)
		if len(tags) >= limit+1 {
			break
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	// return results (optional trimmed) and hasMore
	hasMore := false
	if len(tags) > limit {
		hasMore = true
		tags = tags[:limit]
	}
	return tags, hasMore, nil
}

func (c *cataloger) GetTag(ctx context.Context, repository string, tagID string) (string, error) {
	repositoryID := graveler.RepositoryID(repository)
	tag := graveler.TagID(tagID)
	commit, err := c.EntryCatalog.GetTag(ctx, repositoryID, tag)
	if err != nil {
		return "", err
	}
	return commit.String(), nil
}

// GetEntry returns the current entry for path in repository branch reference.  Returns
// the entry with ExpiredError if it has expired from underlying storage.
func (c *cataloger) GetEntry(ctx context.Context, repository string, reference string, path string, _ catalog.GetEntryParams) (*catalog.Entry, error) {
	repositoryID := graveler.RepositoryID(repository)
	ref := graveler.Ref(reference)
	p := Path(path)
	ent, err := c.EntryCatalog.GetEntry(ctx, repositoryID, ref, p)
	if err != nil {
		return nil, err
	}
	catalogEntry := newCatalogEntryFromEntry(false, p.String(), ent)
	return &catalogEntry, nil
}

func EntryFromCatalogEntry(entry catalog.Entry) *Entry {
	return &Entry{
		Address:      entry.PhysicalAddress,
		Metadata:     entry.Metadata,
		LastModified: timestamppb.New(entry.CreationDate),
		ETag:         entry.Checksum,
		Size:         entry.Size,
	}
}

func (c *cataloger) CreateEntry(ctx context.Context, repository string, branch string, entry catalog.Entry, _ catalog.CreateEntryParams) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	ent := EntryFromCatalogEntry(entry)
	return c.EntryCatalog.SetEntry(ctx, repositoryID, branchID, Path(entry.Path), ent)
}

func (c *cataloger) CreateEntries(ctx context.Context, repository string, branch string, entries []catalog.Entry) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	for _, entry := range entries {
		ent := EntryFromCatalogEntry(entry)
		if err := c.EntryCatalog.SetEntry(ctx, repositoryID, branchID, Path(entry.Path), ent); err != nil {
			return err
		}
	}
	return nil
}

func (c *cataloger) DeleteEntry(ctx context.Context, repository string, branch string, path string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	p := Path(path)
	return c.EntryCatalog.DeleteEntry(ctx, repositoryID, branchID, p)
}

func (c *cataloger) ListEntries(ctx context.Context, repository string, reference string, prefix string, after string, delimiter string, limit int) ([]*catalog.Entry, bool, error) {
	// normalize limit
	if limit < 0 || limit > ListEntriesLimitMax {
		limit = ListEntriesLimitMax
	}
	prefixPath := Path(prefix)
	afterPath := Path(after)
	delimiterPath := Path(delimiter)
	it, err := c.EntryCatalog.ListEntries(ctx, graveler.RepositoryID(repository), graveler.Ref(reference), prefixPath, delimiterPath)
	if err != nil {
		return nil, false, err
	}
	it.SeekGE(afterPath)
	var entries []*catalog.Entry
	for it.Next() {
		v := it.Value()
		if v.Path == afterPath {
			continue
		}
		entry := newCatalogEntryFromEntry(v.CommonPrefix, v.Path.String(), v.Entry)
		entries = append(entries, &entry)
		if len(entries) >= limit+1 {
			break
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	// trim result if needed and return has more
	hasMore := false
	if len(entries) > limit {
		hasMore = true
		entries = entries[:limit]
	}
	return entries, hasMore, nil
}

func (c *cataloger) ResetEntry(ctx context.Context, repository string, branch string, path string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	entryPath := Path(path)
	return c.EntryCatalog.ResetKey(ctx, repositoryID, branchID, entryPath)
}

func (c *cataloger) ResetEntries(ctx context.Context, repository string, branch string, prefix string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	prefixPath := Path(prefix)
	return c.EntryCatalog.ResetPrefix(ctx, repositoryID, branchID, prefixPath)
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
	return c.dummyDedupeCh
}

func (c *cataloger) Commit(ctx context.Context, repository string, branch string, message string, committer string, metadata catalog.Metadata) (*catalog.CommitLog, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
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
	catalogCommitLog.CreationDate = commit.CreationDate.UTC()
	return catalogCommitLog, nil
}

func (c *cataloger) GetCommit(ctx context.Context, repository string, reference string) (*catalog.CommitLog, error) {
	repositoryID := graveler.RepositoryID(repository)
	ref := graveler.Ref(reference)
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
		MetaRangeID:  string(commit.MetaRangeID),
		Metadata:     catalog.Metadata(commit.Metadata),
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, string(parent))
	}
	return catalogCommitLog, nil
}

func (c *cataloger) ListCommits(ctx context.Context, repository string, branch string, fromReference string, limit int) ([]*catalog.CommitLog, bool, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchCommitID, err := c.EntryCatalog.Dereference(ctx, repositoryID, graveler.Ref(branch))
	if err != nil {
		return nil, false, fmt.Errorf("branch ref: %w", err)
	}
	if branchCommitID == "" {
		// return empty log if there is no commit on branch yet
		return make([]*catalog.CommitLog, 0), false, nil
	}
	it, err := c.EntryCatalog.Log(ctx, repositoryID, branchCommitID)
	if err != nil {
		return nil, false, err
	}
	// skip until 'fromReference' if needed
	if fromReference != "" {
		fromCommitID, err := c.EntryCatalog.Dereference(ctx, repositoryID, graveler.Ref(fromReference))
		if err != nil {
			return nil, false, fmt.Errorf("from ref: %w", err)
		}
		for it.Next() {
			if it.Value().CommitID == fromCommitID {
				break
			}
		}
		if err := it.Err(); err != nil {
			return nil, false, err
		}
	}

	// collect commits
	var commits []*catalog.CommitLog
	for it.Next() {
		v := it.Value()
		commit := &catalog.CommitLog{
			Reference:    v.CommitID.String(),
			Committer:    v.Committer,
			Message:      v.Message,
			CreationDate: v.CreationDate,
			Metadata:     map[string]string(v.Metadata),
			MetaRangeID:  string(v.MetaRangeID),
			Parents:      make([]string, 0, len(v.Parents)),
		}
		for _, parent := range v.Parents {
			commit.Parents = append(commit.Parents, parent.String())
		}
		commits = append(commits, commit)
		if len(commits) >= limit+1 {
			break
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	hasMore := false
	if len(commits) > limit {
		hasMore = true
		commits = commits[:limit]
	}
	return commits, hasMore, nil
}

func (c *cataloger) Revert(ctx context.Context, repository string, branch string, reference string, committer string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	ref := graveler.Ref(reference)
	_, _, err := c.EntryCatalog.Revert(ctx, repositoryID, branchID, ref, committer, fmt.Sprintf("Revert %s", reference), nil)
	return err
}

func (c *cataloger) RollbackCommit(_ context.Context, _ string, _ string, _ string) error {
	c.log.Debug("rollback to commit is not supported in rocks implementation")
	return catalog.ErrFeatureNotSupported
}

func (c *cataloger) Diff(ctx context.Context, repository string, leftReference string, rightReference string, params catalog.DiffParams) (catalog.Differences, bool, error) {
	it, err := c.EntryCatalog.Diff(ctx, graveler.RepositoryID(repository), graveler.Ref(leftReference), graveler.Ref(rightReference))
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	return listDiffHelper(it, params.Limit, params.After)
}

func (c *cataloger) Compare(ctx context.Context, repository, leftReference string, rightReference string, params catalog.DiffParams) (catalog.Differences, bool, error) {
	it, err := c.EntryCatalog.Compare(ctx, graveler.RepositoryID(repository), graveler.Ref(leftReference), graveler.Ref(rightReference))
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	return listDiffHelper(it, params.Limit, params.After)
}

func (c *cataloger) DiffUncommitted(ctx context.Context, repository string, branch string, limit int, after string) (catalog.Differences, bool, error) {
	it, err := c.EntryCatalog.DiffUncommitted(ctx, graveler.RepositoryID(repository), graveler.BranchID(branch))
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	return listDiffHelper(it, limit, after)
}

func listDiffHelper(it EntryDiffIterator, limit int, after string) (catalog.Differences, bool, error) {
	if limit < 0 || limit > DiffLimitMax {
		limit = DiffLimitMax
	}
	afterPath := Path(after)
	if afterPath != "" {
		it.SeekGE(afterPath)
	}
	diffs := make(catalog.Differences, 0)
	for it.Next() {
		v := it.Value()
		if v.Path == afterPath {
			continue
		}
		diff, err := newDifferenceFromEntryDiff(v)
		if err != nil {
			return nil, false, fmt.Errorf("[I] %w", err)
		}
		diffs = append(diffs, diff)
		if len(diffs) >= limit+1 {
			break
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	hasMore := false
	if len(diffs) > limit {
		hasMore = true
		diffs = diffs[:limit]
	}
	return diffs, hasMore, nil
}

func (c *cataloger) Merge(ctx context.Context, repository string, leftBranch string, rightBranch string, committer string, message string, metadata catalog.Metadata) (*catalog.MergeResult, error) {
	repositoryID := graveler.RepositoryID(repository)
	leftRef := graveler.Ref(leftBranch)
	rightBranchID := graveler.BranchID(rightBranch)
	meta := graveler.Metadata(metadata)
	commitID, summary, err := c.EntryCatalog.Merge(ctx, repositoryID, leftRef, rightBranchID, committer, message, meta)
	if errors.Is(err, graveler.ErrConflictFound) {
		// for compatibility with old cataloger
		return &catalog.MergeResult{
			Summary: map[catalog.DifferenceType]int{catalog.DifferenceTypeConflict: 1},
		}, err
	}
	if err != nil {
		return nil, err
	}
	count := make(map[catalog.DifferenceType]int)
	for k, v := range summary.Count {
		kk, err := catalogDiffType(k)
		if err != nil {
			return nil, err
		}
		count[kk] = v
	}
	return &catalog.MergeResult{
		Summary:   count,
		Reference: commitID.String(),
	}, nil
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
	close(c.dummyDedupeCh)
	return nil
}

func newCatalogEntryFromEntry(commonPrefix bool, path string, ent *Entry) catalog.Entry {
	catEnt := catalog.Entry{
		CommonLevel: commonPrefix,
		Path:        path,
	}
	if ent != nil {
		catEnt.PhysicalAddress = ent.Address
		catEnt.CreationDate = ent.LastModified.AsTime()
		catEnt.Size = ent.Size
		catEnt.Checksum = ent.ETag
		catEnt.Metadata = ent.Metadata
		catEnt.Expired = false
	}
	return catEnt
}

func catalogDiffType(typ graveler.DiffType) (catalog.DifferenceType, error) {
	switch typ {
	case graveler.DiffTypeAdded:
		return catalog.DifferenceTypeAdded, nil
	case graveler.DiffTypeRemoved:
		return catalog.DifferenceTypeRemoved, nil
	case graveler.DiffTypeChanged:
		return catalog.DifferenceTypeChanged, nil
	case graveler.DiffTypeConflict:
		return catalog.DifferenceTypeConflict, nil
	default:
		return catalog.DifferenceTypeNone, fmt.Errorf("%d: %w", typ, ErrUnknownDiffType)
	}
}

func newDifferenceFromEntryDiff(v *EntryDiff) (catalog.Difference, error) {
	var (
		diff catalog.Difference
		err  error
	)
	diff.Entry = newCatalogEntryFromEntry(false, v.Path.String(), v.Entry)
	diff.Type, err = catalogDiffType(v.Type)
	return diff, err
}
