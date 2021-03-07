package catalog

import (
	"context"
	"crypto"
	_ "crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"

	"github.com/cockroachdb/pebble"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/ref"
	"github.com/treeverse/lakefs/graveler/sstable"
	"github.com/treeverse/lakefs/graveler/staging"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/pyramid"
	"github.com/treeverse/lakefs/pyramid/params"

	"github.com/treeverse/lakefs/block"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// hashAlg is the hashing algorithm to use to generate graveler identifiers.  Changing it
// causes all old identifiers to change, so while existing installations will continue to
// function they will be unable to re-use any existing objects.
const hashAlg = crypto.SHA256

type Path string

type EntryRecord struct {
	Path Path
	*Entry
}

type EntryListing struct {
	CommonPrefix bool
	Path
	*Entry
}

type EntryDiff struct {
	Type  graveler.DiffType
	Path  Path
	Entry *Entry
}

type EntryIterator interface {
	Next() bool
	SeekGE(id Path)
	Value() *EntryRecord
	Err() error
	Close()
}

type EntryListingIterator interface {
	Next() bool
	SeekGE(id Path)
	Value() *EntryListing
	Err() error
	Close()
}

type EntryDiffIterator interface {
	Next() bool
	SeekGE(id Path)
	Value() *EntryDiff
	Err() error
	Close()
}

func (id Path) String() string {
	return string(id)
}

type Store interface {
	graveler.KeyValueStore
	graveler.VersionController
	graveler.Dumper
	graveler.Loader
	graveler.Plumbing
}

const (
	RangeFSName     = "range"
	MetaRangeFSName = "meta-range"
)

type Config struct {
	Config *config.Config
	DB     db.Database
	LockDB db.Database
}

type Catalog struct {
	BlockAdapter block.Adapter
	Store        Store
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

func New(ctx context.Context, cfg Config) (*Catalog, error) {
	if cfg.LockDB == nil {
		cfg.LockDB = cfg.DB
	}

	tierFSParams, err := cfg.Config.GetCommittedTierFSParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("configure tiered FS for committed: %w", err)
	}
	metaRangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              MetaRangeFSName,
		DiskAllocProportion: tierFSParams.MetaRangeAllocationProportion,
	})
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed metaranges: %w", err)
	}

	rangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              RangeFSName,
		DiskAllocProportion: tierFSParams.RangeAllocationProportion,
	})
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed ranges: %w", err)
	}

	pebbleSSTableCache := pebble.NewCache(tierFSParams.PebbleSSTableCacheSizeBytes)
	defer pebbleSSTableCache.Unref()

	sstableManager := sstable.NewPebbleSSTableRangeManager(pebbleSSTableCache, rangeFS, hashAlg)
	sstableMetaManager := sstable.NewPebbleSSTableRangeManager(pebbleSSTableCache, metaRangeFS, hashAlg)
	sstableMetaRangeManager, err := committed.NewMetaRangeManager(
		*cfg.Config.GetCommittedParams(),
		// TODO(ariels): Use separate range managers for metaranges and ranges
		sstableMetaManager,
		sstableManager,
	)
	if err != nil {
		return nil, fmt.Errorf("create SSTable-based metarange manager: %w", err)
	}
	committedManager := committed.NewCommittedManager(sstableMetaRangeManager)

	stagingManager := staging.NewManager(cfg.DB)
	refManager := ref.NewPGRefManager(cfg.DB, ident.NewHexAddressProvider())
	branchLocker := ref.NewBranchLocker(cfg.LockDB)
	store := graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager)

	return &Catalog{
		BlockAdapter: tierFSParams.Adapter,
		Store:        store,
		log:          logging.Default().WithField("service_name", "entry_catalog"),
	}, nil
}

func (c *Catalog) SetHooksHandler(hooks graveler.HooksHandler) {
	c.Store.SetHooksHandler(hooks)
}

// CreateRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with default branch name 'branch'
func (c *Catalog) CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) (*Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	storageNS := graveler.StorageNamespace(storageNamespace)
	branchID := graveler.BranchID(branch)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"storageNamespace", storageNS, ValidateStorageNamespace},
	}); err != nil {
		return nil, err
	}
	repo, err := c.Store.CreateRepository(ctx, repositoryID, storageNS, branchID)
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
func (c *Catalog) CreateBareRepository(ctx context.Context, repository string, storageNamespace string, defaultBranchID string) (*Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	storageNS := graveler.StorageNamespace(storageNamespace)
	branchID := graveler.BranchID(defaultBranchID)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"storageNamespace", storageNS, ValidateStorageNamespace},
	}); err != nil {
		return nil, err
	}
	repo, err := c.Store.CreateBareRepository(ctx, repositoryID, storageNS, branchID)
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
func (c *Catalog) GetRepository(ctx context.Context, repository string) (*Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	repo, err := c.Store.GetRepository(ctx, repositoryID)
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
func (c *Catalog) DeleteRepository(ctx context.Context, repository string) error {
	repositoryID := graveler.RepositoryID(repository)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return err
	}
	return c.Store.DeleteRepository(ctx, repositoryID)
}

// ListRepositories list repositories information, the bool returned is true when more repositories can be listed.
// In this case pass the last repository name as 'after' on the next call to ListRepositories
func (c *Catalog) ListRepositories(ctx context.Context, limit int, after string) ([]*Repository, bool, error) {
	// normalize limit
	if limit < 0 || limit > ListRepositoriesLimitMax {
		limit = ListRepositoriesLimitMax
	}
	// get list repositories iterator
	it, err := c.Store.ListRepositories(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("get iterator: %w", err)
	}
	defer it.Close()
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

func (c *Catalog) CreateBranch(ctx context.Context, repository string, branch string, sourceBranch string) (*CommitLog, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	sourceRef := graveler.Ref(sourceBranch)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"ref", sourceRef, ValidateRef},
	}); err != nil {
		return nil, err
	}
	newBranch, err := c.Store.CreateBranch(ctx, repositoryID, branchID, sourceRef)
	if err != nil {
		return nil, err
	}
	commit, err := c.Store.GetCommit(ctx, repositoryID, newBranch.CommitID)
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

func (c *Catalog) DeleteBranch(ctx context.Context, repository string, branch string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return err
	}
	return c.Store.DeleteBranch(ctx, repositoryID, branchID)
}

func (c *Catalog) ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*Branch, bool, error) {
	repositoryID := graveler.RepositoryID(repository)
	afterBranch := graveler.BranchID(after)
	prefixBranch := graveler.BranchID(prefix)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return nil, false, err
	}
	// normalize limit
	if limit < 0 || limit > ListBranchesLimitMax {
		limit = ListBranchesLimitMax
	}
	it, err := c.Store.ListBranches(ctx, repositoryID)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
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

func (c *Catalog) BranchExists(ctx context.Context, repository string, branch string) (bool, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return false, err
	}
	_, err := c.Store.GetBranch(ctx, repositoryID, branchID)
	if errors.Is(err, graveler.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *Catalog) GetBranchReference(ctx context.Context, repository string, branch string) (string, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return "", err
	}
	b, err := c.Store.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return "", err
	}
	return string(b.CommitID), nil
}

func (c *Catalog) ResetBranch(ctx context.Context, repository string, branch string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return err
	}
	return c.Store.Reset(ctx, repositoryID, branchID)
}

func (c *Catalog) CreateTag(ctx context.Context, repository string, tagID string, ref string) (string, error) {
	repositoryID := graveler.RepositoryID(repository)
	tag := graveler.TagID(tagID)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"tagID", tag, ValidateTagID},
	}); err != nil {
		return "", err
	}
	commitID, err := c.Store.Dereference(ctx, repositoryID, graveler.Ref(ref))
	if err != nil {
		return "", err
	}
	err = c.Store.CreateTag(ctx, repositoryID, tag, commitID)
	if err != nil {
		return "", err
	}
	return commitID.String(), nil
}

func (c *Catalog) DeleteTag(ctx context.Context, repository string, tagID string) error {
	repositoryID := graveler.RepositoryID(repository)
	tag := graveler.TagID(tagID)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"tagID", tag, ValidateTagID},
	}); err != nil {
		return err
	}
	return c.Store.DeleteTag(ctx, repositoryID, tag)
}

func (c *Catalog) ListTags(ctx context.Context, repository string, limit int, after string) ([]*Tag, bool, error) {
	if limit < 0 || limit > ListTagsLimitMax {
		limit = ListTagsLimitMax
	}
	repositoryID := graveler.RepositoryID(repository)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return nil, false, err
	}
	it, err := c.Store.ListTags(ctx, repositoryID)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
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

func (c *Catalog) GetTag(ctx context.Context, repository string, tagID string) (string, error) {
	repositoryID := graveler.RepositoryID(repository)
	tag := graveler.TagID(tagID)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"tagID", tag, ValidateTagID},
	}); err != nil {
		return "", err
	}
	commit, err := c.Store.GetTag(ctx, repositoryID, tag)
	if err != nil {
		return "", err
	}
	return commit.String(), nil
}

// GetEntry returns the current entry for path in repository branch reference.  Returns
// the entry with ExpiredError if it has expired from underlying storage.
func (c *Catalog) GetEntry(ctx context.Context, repository string, reference string, path string, _ GetEntryParams) (*DBEntry, error) {
	repositoryID := graveler.RepositoryID(repository)
	ref := graveler.Ref(reference)
	p := Path(path)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"ref", ref, ValidateRef},
		{"path", p, ValidatePath},
	}); err != nil {
		return nil, err
	}
	val, err := c.Store.Get(ctx, repositoryID, ref, graveler.Key(p))
	if err != nil {
		return nil, err
	}
	ent, err := ValueToEntry(val)
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

func (c *Catalog) CreateEntry(ctx context.Context, repository string, branch string, entry DBEntry) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	ent := EntryFromCatalogEntry(entry)
	path := Path(entry.Path)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"path", path, ValidatePath},
	}); err != nil {
		return err
	}
	key := graveler.Key(path)
	value, err := EntryToValue(ent)
	if err != nil {
		return err
	}
	return c.Store.Set(ctx, repositoryID, branchID, key, *value)
}

func (c *Catalog) CreateEntries(ctx context.Context, repository string, branch string, entries []DBEntry) error {
	for _, entry := range entries {
		if err := c.CreateEntry(ctx, repository, branch, entry); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) DeleteEntry(ctx context.Context, repository string, branch string, path string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	p := Path(path)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"path", p, ValidatePath},
	}); err != nil {
		return err
	}
	key := graveler.Key(p)
	return c.Store.Delete(ctx, repositoryID, branchID, key)
}

func (c *Catalog) ListEntries(ctx context.Context, repository string, reference string, prefix string, after string, delimiter string, limit int) ([]*DBEntry, bool, error) {
	// normalize limit
	if limit < 0 || limit > ListEntriesLimitMax {
		limit = ListEntriesLimitMax
	}
	prefixPath := Path(prefix)
	afterPath := Path(after)
	delimiterPath := Path(delimiter)
	repositoryID := graveler.RepositoryID(repository)
	ref := graveler.Ref(reference)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"ref", ref, ValidateRef},
		{"prefix", prefixPath, ValidatePathOptional},
		{"delimiter", delimiterPath, ValidatePathOptional},
	}); err != nil {
		return nil, false, err
	}
	iter, err := c.Store.List(ctx, repositoryID, ref)
	if err != nil {
		return nil, false, err
	}
	it := NewEntryListingIterator(NewValueToEntryIterator(iter), prefixPath, delimiterPath)
	defer it.Close()

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

func (c *Catalog) ResetEntry(ctx context.Context, repository string, branch string, path string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	entryPath := Path(path)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"path", entryPath, ValidatePath},
	}); err != nil {
		return err
	}
	key := graveler.Key(entryPath)
	return c.Store.ResetKey(ctx, repositoryID, branchID, key)
}

func (c *Catalog) ResetEntries(ctx context.Context, repository string, branch string, prefix string) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	prefixPath := Path(prefix)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return err
	}
	keyPrefix := graveler.Key(prefixPath)
	return c.Store.ResetPrefix(ctx, repositoryID, branchID, keyPrefix)
}

func (c *Catalog) Commit(ctx context.Context, repository string, branch string, message string, committer string, metadata Metadata) (*CommitLog, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return nil, err
	}
	commitID, err := c.Store.Commit(ctx, repositoryID, branchID, graveler.CommitParams{
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
	commit, err := c.Store.GetCommit(ctx, repositoryID, commitID)
	if err != nil {
		return catalogCommitLog, graveler.ErrCommitNotFound
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, parent.String())
	}
	catalogCommitLog.CreationDate = commit.CreationDate.UTC()
	return catalogCommitLog, nil
}

func (c *Catalog) GetCommit(ctx context.Context, repository string, reference string) (*CommitLog, error) {
	repositoryID := graveler.RepositoryID(repository)
	ref := graveler.Ref(reference)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	commitID, err := c.Store.Dereference(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}

	commit, err := c.Store.GetCommit(ctx, repositoryID, commitID)
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

func (c *Catalog) ListCommits(ctx context.Context, repository string, branch string, fromReference string, limit int) ([]*CommitLog, bool, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchRef := graveler.BranchID(branch)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branch", branchRef, ValidateBranchID},
	}); err != nil {
		return nil, false, err
	}
	branchCommitID, err := c.Store.Dereference(ctx, repositoryID, graveler.Ref(branchRef))
	if err != nil {
		return nil, false, fmt.Errorf("branch ref: %w", err)
	}
	if branchCommitID == "" {
		// return empty log if there is no commit on branch yet
		return make([]*CommitLog, 0), false, nil
	}
	it, err := c.Store.Log(ctx, repositoryID, branchCommitID)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	// skip until 'fromReference' if needed
	if fromReference != "" {
		fromCommitID, err := c.Store.Dereference(ctx, repositoryID, graveler.Ref(fromReference))
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

func (c *Catalog) Revert(ctx context.Context, repository string, branch string, params RevertParams) error {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	ref := graveler.Ref(params.Reference)
	commitParams := graveler.CommitParams{
		Committer: params.Committer,
		Message:   fmt.Sprintf("Revert %s", params.Reference),
	}
	parentNumber := params.ParentNumber
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"ref", ref, ValidateRef},
		{"committer", commitParams.Committer, ValidateRequiredString},
		{"message", commitParams.Message, ValidateRequiredString},
		{"parentNumber", parentNumber, ValidateNonNegativeInt},
	}); err != nil {
		return err
	}
	_, _, err := c.Store.Revert(ctx, repositoryID, branchID, ref, parentNumber, commitParams)
	return err
}

func (c *Catalog) RollbackCommit(_ context.Context, _ string, _ string, _ string) error {
	c.log.Debug("rollback to commit is not supported in rocks implementation")
	return ErrFeatureNotSupported
}

func (c *Catalog) Diff(ctx context.Context, repository string, leftReference string, rightReference string, params DiffParams) (Differences, bool, error) {
	repositoryID := graveler.RepositoryID(repository)
	left := graveler.Ref(leftReference)
	right := graveler.Ref(rightReference)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"left", left, ValidateRef},
		{"right", right, ValidateRef},
	}); err != nil {
		return nil, false, err
	}
	iter, err := c.Store.Diff(ctx, repositoryID, left, right)
	if err != nil {
		return nil, false, err
	}
	it := NewEntryDiffIterator(iter)
	defer it.Close()
	return listDiffHelper(it, params.Limit, params.After)
}

func (c *Catalog) Compare(ctx context.Context, repository, leftReference string, rightReference string, params DiffParams) (Differences, bool, error) {
	repositoryID := graveler.RepositoryID(repository)
	from := graveler.Ref(leftReference)
	to := graveler.Ref(rightReference)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"from", from, ValidateRef},
		{"to", to, ValidateRef},
	}); err != nil {
		return nil, false, err
	}
	iter, err := c.Store.Compare(ctx, repositoryID, from, to)
	if err != nil {
		return nil, false, err
	}
	it := NewEntryDiffIterator(iter)
	defer it.Close()
	return listDiffHelper(it, params.Limit, params.After)
}

func (c *Catalog) DiffUncommitted(ctx context.Context, repository string, branch string, limit int, after string) (Differences, bool, error) {
	repositoryID := graveler.RepositoryID(repository)
	branchID := graveler.BranchID(branch)
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return nil, false, err
	}
	iter, err := c.Store.DiffUncommitted(ctx, repositoryID, branchID)
	if err != nil {
		return nil, false, err
	}
	it := NewEntryDiffIterator(iter)
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

func (c *Catalog) Merge(ctx context.Context, repository string, destinationBranch string, sourceRef string, committer string, message string, metadata Metadata) (*MergeResult, error) {
	repositoryID := graveler.RepositoryID(repository)
	destination := graveler.BranchID(destinationBranch)
	source := graveler.Ref(sourceRef)
	meta := graveler.Metadata(metadata)
	commitParams := graveler.CommitParams{
		Committer: committer,
		Message:   message,
		Metadata:  meta,
	}
	if commitParams.Message == "" {
		commitParams.Message = fmt.Sprintf("Merge '%s' into '%s'", source, destination)
	}
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"destination", destination, ValidateBranchID},
		{"source", source, ValidateRef},
		{"committer", commitParams.Committer, ValidateRequiredString},
		{"message", commitParams.Message, ValidateRequiredString},
	}); err != nil {
		return nil, err
	}
	commitID, summary, err := c.Store.Merge(ctx, repositoryID, destination, source, commitParams)
	if errors.Is(err, graveler.ErrConflictFound) {
		// for compatibility with old Catalog
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

func (c *Catalog) DumpCommits(ctx context.Context, repositoryID string) (string, error) {
	metaRangeID, err := c.Store.DumpCommits(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	return string(*metaRangeID), nil
}

func (c *Catalog) DumpBranches(ctx context.Context, repositoryID string) (string, error) {
	metaRangeID, err := c.Store.DumpBranches(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	return string(*metaRangeID), nil
}

func (c *Catalog) DumpTags(ctx context.Context, repositoryID string) (string, error) {
	metaRangeID, err := c.Store.DumpTags(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	return string(*metaRangeID), nil
}

func (c *Catalog) LoadCommits(ctx context.Context, repositoryID, commitsMetaRangeID string) error {
	return c.Store.LoadCommits(ctx, graveler.RepositoryID(repositoryID), graveler.MetaRangeID(commitsMetaRangeID))
}

func (c *Catalog) LoadBranches(ctx context.Context, repositoryID, branchesMetaRangeID string) error {
	return c.Store.LoadBranches(ctx, graveler.RepositoryID(repositoryID), graveler.MetaRangeID(branchesMetaRangeID))
}

func (c *Catalog) LoadTags(ctx context.Context, repositoryID, tagsMetaRangeID string) error {
	return c.Store.LoadTags(ctx, graveler.RepositoryID(repositoryID), graveler.MetaRangeID(tagsMetaRangeID))
}

func (c *Catalog) GetMetaRange(ctx context.Context, repositoryID, metaRangeID string) (graveler.MetaRangeInfo, error) {
	return c.Store.GetMetaRange(ctx, graveler.RepositoryID(repositoryID), graveler.MetaRangeID(metaRangeID))
}

func (c *Catalog) GetRange(ctx context.Context, repositoryID, rangeID string) (graveler.RangeInfo, error) {
	return c.Store.GetRange(ctx, graveler.RepositoryID(repositoryID), graveler.RangeID(rangeID))
}

func (c *Catalog) Close() error {
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
