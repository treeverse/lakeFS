package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type cataloger struct {
	EntryCatalog *EntryCatalog
	log          logging.Logger
}

const (
	ListRepositoriesLimitMax = 1000
	ListBranchesLimitMax     = 1000
	ListTagsLimitMax         = 1000
	DiffLimitMax             = 1000
	ListEntriesLimitMax      = 10000
)

var ErrUnknownDiffType = errors.New("unknown graveler difference type")

func NewCataloger(cfg Config) (Cataloger, error) {
	entryCatalog, err := NewEntryCatalog(cfg)
	if err != nil {
		return nil, err
	}
	return &cataloger{
		EntryCatalog: entryCatalog,
		log:          logging.Default().WithField("service_name", "entry_catalog"),
	}, nil
}

// CreateRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with default branch name 'branch'
func (c *cataloger) CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) (*Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	storageNS := graveler.StorageNamespace(storageNamespace)
	branchID := graveler.BranchID(branch)
	repo, err := c.EntryCatalog.CreateRepository(ctx, repositoryID, storageNS, branchID)
	if err != nil {
		return nil, err
	}
	catalogRepo := &Repository{
		Name:             repositoryID.String(),
		StorageNamespace: storageNS.String(),
		DefaultBranch:    branchID.String(),
		CreationDate:     repo.CreationDate,
	}
	return catalogRepo, nil
}

// CreateBareRepository creates a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with no initial branch or commit
func (c *cataloger) CreateBareRepository(ctx context.Context, repository string, storageNamespace string, defaultBranchID string) (*Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	storageNS := graveler.StorageNamespace(storageNamespace)
	branchID := graveler.BranchID(defaultBranchID)
	repo, err := c.EntryCatalog.CreateBareRepository(ctx, repositoryID, storageNS, branchID)
	if err != nil {
		return nil, err
	}
	catalogRepo := &Repository{
		Name:             repositoryID.String(),
		StorageNamespace: storageNS.String(),
		DefaultBranch:    branchID.String(),
		CreationDate:     repo.CreationDate,
	}
	return catalogRepo, nil
}

// GetRepository get repository information
func (c *cataloger) GetRepository(ctx context.Context, repository string) (*Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	repo, err := c.EntryCatalog.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	catalogRepository := &Repository{
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
func (c *cataloger) ListRepositories(ctx context.Context, limit int, after string) ([]*Repository, bool, error) {
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

	var repos []*Repository
	for it.Next() {
		record := it.Value()
		if record.RepositoryID == afterRepositoryID {
			continue
		}
		repos = append(repos, &Repository{
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

func (c *cataloger) CreateBranch(ctx context.Context, repository string, branch string, sourceBranch string) (*CommitLog, error) {
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
	catalogCommitLog := &CommitLog{
		Reference: newBranch.CommitID.String(),
		Committer: commit.Committer,
		Message:   commit.Message,
		Metadata:  Metadata(commit.Metadata),
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

func (c *cataloger) ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*Branch, bool, error) {
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
	var branches []*Branch
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
		branch := &Branch{
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

func (c *cataloger) ListTags(ctx context.Context, repository string, limit int, after string) ([]*Tag, bool, error) {
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

	var tags []*Tag
	for it.Next() {
		v := it.Value()
		if v.TagID == afterTagID {
			continue
		}
		branch := &Tag{
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
func (c *cataloger) GetEntry(ctx context.Context, repository string, reference string, path string, _ GetEntryParams) (*DBEntry, error) {
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

func EntryFromCatalogEntry(entry DBEntry) *Entry {
	return &Entry{
		Address:      entry.PhysicalAddress,
		Metadata:     entry.Metadata,
		LastModified: timestamppb.New(entry.CreationDate),
		ETag:         entry.Checksum,
		Size:         entry.Size,
	}
}

func (c *cataloger) CreateEntry(ctx context.Context, repository string, branch string, entry DBEntry) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	ent := EntryFromCatalogEntry(entry)
	return c.EntryCatalog.SetEntry(ctx, repositoryID, branchID, Path(entry.Path), ent)
}

func (c *cataloger) CreateEntries(ctx context.Context, repository string, branch string, entries []DBEntry) error {
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

func (c *cataloger) ListEntries(ctx context.Context, repository string, reference string, prefix string, after string, delimiter string, limit int) ([]*DBEntry, bool, error) {
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
	var entries []*DBEntry
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

func (c *cataloger) Commit(ctx context.Context, repository string, branch string, message string, committer string, metadata Metadata) (*CommitLog, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	commitID, err := c.EntryCatalog.Commit(ctx, repositoryID, branchID, graveler.CommitParams{
		Committer: committer,
		Message:   message,
		Metadata:  map[string]string(metadata),
	})
	if err != nil {
		return nil, err
	}
	catalogCommitLog := &CommitLog{
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

func (c *cataloger) GetCommit(ctx context.Context, repository string, reference string) (*CommitLog, error) {
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
	catalogCommitLog := &CommitLog{
		Reference:    ref.String(),
		Committer:    commit.Committer,
		Message:      commit.Message,
		CreationDate: commit.CreationDate,
		MetaRangeID:  string(commit.MetaRangeID),
		Metadata:     Metadata(commit.Metadata),
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, string(parent))
	}
	return catalogCommitLog, nil
}

func (c *cataloger) ListCommits(ctx context.Context, repository string, branch string, fromReference string, limit int) ([]*CommitLog, bool, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchCommitID, err := c.EntryCatalog.Dereference(ctx, repositoryID, graveler.Ref(branch))
	if err != nil {
		return nil, false, fmt.Errorf("branch ref: %w", err)
	}
	if branchCommitID == "" {
		// return empty log if there is no commit on branch yet
		return make([]*CommitLog, 0), false, nil
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
	var commits []*CommitLog
	for it.Next() {
		v := it.Value()
		commit := &CommitLog{
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

func (c *cataloger) Revert(ctx context.Context, repository string, branch string, params RevertParams) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	ref := graveler.Ref(params.Reference)
	_, _, err := c.EntryCatalog.Revert(ctx, repositoryID, branchID, ref, params.ParentNumber, graveler.CommitParams{
		Committer: params.Committer,
		Message:   fmt.Sprintf("Revert %s", params.Reference),
	})
	return err
}

func (c *cataloger) RollbackCommit(_ context.Context, _ string, _ string, _ string) error {
	c.log.Debug("rollback to commit is not supported in rocks implementation")
	return ErrFeatureNotSupported
}

func (c *cataloger) Diff(ctx context.Context, repository string, leftReference string, rightReference string, params DiffParams) (Differences, bool, error) {
	it, err := c.EntryCatalog.Diff(ctx, graveler.RepositoryID(repository), graveler.Ref(leftReference), graveler.Ref(rightReference))
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	return listDiffHelper(it, params.Limit, params.After)
}

func (c *cataloger) Compare(ctx context.Context, repository, leftReference string, rightReference string, params DiffParams) (Differences, bool, error) {
	it, err := c.EntryCatalog.Compare(ctx, graveler.RepositoryID(repository), graveler.Ref(leftReference), graveler.Ref(rightReference))
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	return listDiffHelper(it, params.Limit, params.After)
}

func (c *cataloger) DiffUncommitted(ctx context.Context, repository string, branch string, limit int, after string) (Differences, bool, error) {
	it, err := c.EntryCatalog.DiffUncommitted(ctx, graveler.RepositoryID(repository), graveler.BranchID(branch))
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	return listDiffHelper(it, limit, after)
}

func listDiffHelper(it EntryDiffIterator, limit int, after string) (Differences, bool, error) {
	if limit < 0 || limit > DiffLimitMax {
		limit = DiffLimitMax
	}
	afterPath := Path(after)
	if afterPath != "" {
		it.SeekGE(afterPath)
	}
	diffs := make(Differences, 0)
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

func (c *cataloger) Merge(ctx context.Context, repository string, destinationBranch string, sourceRef string, committer string, message string, metadata Metadata) (*MergeResult, error) {
	repositoryID := graveler.RepositoryID(repository)
	dest := graveler.BranchID(destinationBranch)
	source := graveler.Ref(sourceRef)
	meta := graveler.Metadata(metadata)
	commitID, summary, err := c.EntryCatalog.Merge(ctx, repositoryID, dest, source, graveler.CommitParams{
		Committer: committer,
		Message:   message,
		Metadata:  meta,
	})
	if errors.Is(err, graveler.ErrConflictFound) {
		// for compatibility with old cataloger
		return &MergeResult{
			Summary: map[DifferenceType]int{DifferenceTypeConflict: 1},
		}, err
	}
	if err != nil {
		return nil, err
	}
	count := make(map[DifferenceType]int)
	for k, v := range summary.Count {
		kk, err := catalogDiffType(k)
		if err != nil {
			return nil, err
		}
		count[kk] = v
	}
	return &MergeResult{
		Summary:   count,
		Reference: commitID.String(),
	}, nil
}

func (c *cataloger) DumpCommits(ctx context.Context, repositoryID string) (string, error) {
	metaRangeID, err := c.EntryCatalog.DumpCommits(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	return string(*metaRangeID), nil
}

func (c *cataloger) DumpBranches(ctx context.Context, repositoryID string) (string, error) {
	metaRangeID, err := c.EntryCatalog.DumpBranches(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	return string(*metaRangeID), nil
}

func (c *cataloger) DumpTags(ctx context.Context, repositoryID string) (string, error) {
	metaRangeID, err := c.EntryCatalog.DumpTags(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	return string(*metaRangeID), nil
}

func (c *cataloger) LoadCommits(ctx context.Context, repositoryID, commitsMetaRangeID string) error {
	return c.EntryCatalog.LoadCommits(ctx, graveler.RepositoryID(repositoryID), graveler.MetaRangeID(commitsMetaRangeID))
}

func (c *cataloger) LoadBranches(ctx context.Context, repositoryID, branchesMetaRangeID string) error {
	return c.EntryCatalog.LoadBranches(ctx, graveler.RepositoryID(repositoryID), graveler.MetaRangeID(branchesMetaRangeID))
}

func (c *cataloger) LoadTags(ctx context.Context, repositoryID, tagsMetaRangeID string) error {
	return c.EntryCatalog.LoadTags(ctx, graveler.RepositoryID(repositoryID), graveler.MetaRangeID(tagsMetaRangeID))
}

func (c *cataloger) GetMetaRange(ctx context.Context, repositoryID, metaRangeID string) (graveler.MetaRangeData, error) {
	return c.EntryCatalog.GetMetaRange(ctx, graveler.RepositoryID(repositoryID), graveler.MetaRangeID(metaRangeID))
}

func (c *cataloger) GetRange(ctx context.Context, repositoryID, rangeID string) (graveler.RangeData, error) {
	return c.EntryCatalog.GetRange(ctx, graveler.RepositoryID(repositoryID), graveler.RangeID(rangeID))
}

func (c *cataloger) Close() error {
	return nil
}

func newCatalogEntryFromEntry(commonPrefix bool, path string, ent *Entry) DBEntry {
	catEnt := DBEntry{
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

func catalogDiffType(typ graveler.DiffType) (DifferenceType, error) {
	switch typ {
	case graveler.DiffTypeAdded:
		return DifferenceTypeAdded, nil
	case graveler.DiffTypeRemoved:
		return DifferenceTypeRemoved, nil
	case graveler.DiffTypeChanged:
		return DifferenceTypeChanged, nil
	case graveler.DiffTypeConflict:
		return DifferenceTypeConflict, nil
	default:
		return DifferenceTypeNone, fmt.Errorf("%d: %w", typ, ErrUnknownDiffType)
	}
}

func newDifferenceFromEntryDiff(v *EntryDiff) (Difference, error) {
	var (
		diff Difference
		err  error
	)
	diff.DBEntry = newCatalogEntryFromEntry(false, v.Path.String(), v.Entry)
	diff.Type, err = catalogDiffType(v.Type)
	return diff, err
}
