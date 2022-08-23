package catalog

import (
	"bytes"
	"context"
	"crypto"
	_ "crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-multierror"
	"github.com/hnlq715/golang-lru/simplelru"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/branch"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/graveler/retention"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/graveler/sstable"
	"github.com/treeverse/lakefs/pkg/graveler/staging"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/ingest/store"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/pyramid"
	"github.com/treeverse/lakefs/pkg/pyramid/params"
	"github.com/treeverse/lakefs/pkg/validator"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// hashAlg is the hashing algorithm to use to generate graveler identifiers.  Changing it
// causes all old identifiers to change, so while existing installations will continue to
// function they will be unable to re-use any existing objects.
const hashAlg = crypto.SHA256

const NumberOfParentsOfNonMergeCommit = 1

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
	Config        *config.Config
	DB            db.Database
	LockDB        db.Database
	KVStore       *kv.StoreMessage
	WalkerFactory WalkerFactory
}

type Catalog struct {
	BlockAdapter  block.Adapter
	Store         Store
	log           logging.Logger
	walkerFactory WalkerFactory
	managers      []io.Closer
}

const (
	ListRepositoriesLimitMax = 1000
	ListBranchesLimitMax     = 1000
	ListTagsLimitMax         = 1000
	DiffLimitMax             = 1000
	ListEntriesLimitMax      = 10000
)

var ErrUnknownDiffType = errors.New("unknown graveler difference type")

type ctxCloser struct {
	close context.CancelFunc
}

func (c *ctxCloser) Close() error {
	go c.close()
	return nil
}

func New(ctx context.Context, cfg Config) (*Catalog, error) {
	if cfg.LockDB == nil {
		cfg.LockDB = cfg.DB
	}

	ctx, cancelFn := context.WithCancel(ctx)
	adapter, err := factory.BuildBlockAdapter(ctx, nil, cfg.Config)
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("build block adapter: %w", err)
	}
	if cfg.WalkerFactory == nil {
		cfg.WalkerFactory = store.NewFactory(cfg.Config)
	}

	tierFSParams, err := cfg.Config.GetCommittedTierFSParams(adapter)
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("configure tiered FS for committed: %w", err)
	}
	metaRangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              MetaRangeFSName,
		DiskAllocProportion: tierFSParams.MetaRangeAllocationProportion,
	})
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("create tiered FS for committed metaranges: %w", err)
	}

	rangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              RangeFSName,
		DiskAllocProportion: tierFSParams.RangeAllocationProportion,
	})
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("create tiered FS for committed ranges: %w", err)
	}

	pebbleSSTableCache := pebble.NewCache(tierFSParams.PebbleSSTableCacheSizeBytes)
	defer pebbleSSTableCache.Unref()

	sstableManager := sstable.NewPebbleSSTableRangeManager(pebbleSSTableCache, rangeFS, hashAlg)
	sstableMetaManager := sstable.NewPebbleSSTableRangeManager(pebbleSSTableCache, metaRangeFS, hashAlg)

	committedParams := *cfg.Config.GetCommittedParams()
	sstableMetaRangeManager, err := committed.NewMetaRangeManager(
		committedParams,
		// TODO(ariels): Use separate range managers for metaranges and ranges
		sstableMetaManager,
		sstableManager,
	)
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("create SSTable-based metarange manager: %w", err)
	}
	committedManager := committed.NewCommittedManager(sstableMetaRangeManager, sstableManager, committedParams)

	executor := batch.NewConditionalExecutor(logging.Default())
	go executor.Run(ctx)

	var gStore Store
	var stagingManager graveler.StagingManager
	var refManager graveler.RefManager
	var gcManager graveler.GarbageCollectionManager
	var protectedBranchesManager graveler.ProtectedBranchesManager
	branchLocker := ref.NewBranchLocker(cfg.LockDB) // TODO (niro): Will not be needed in KV implementation

	if cfg.Config.GetDatabaseParams().KVEnabled { // TODO (niro): Each module should be replaced by an appropriate KV implementation
		refManager = ref.NewKVRefManager(executor, *cfg.KVStore, ident.NewHexAddressProvider())
		gcManager = retention.NewGarbageCollectionManager(tierFSParams.Adapter, refManager, cfg.Config.GetCommittedBlockStoragePrefix())
		settingManager := settings.NewManager(refManager, *cfg.KVStore)
		protectedBranchesManager = branch.NewProtectionManager(settingManager)
		stagingManager = staging.NewManager(ctx, *cfg.KVStore)
		gStore = graveler.NewKVGraveler(committedManager, stagingManager, refManager, gcManager, protectedBranchesManager)
	} else {
		refManager = ref.NewPGRefManager(executor, cfg.DB, ident.NewHexAddressProvider())
		gcManager = retention.NewGarbageCollectionManager(tierFSParams.Adapter, refManager, cfg.Config.GetCommittedBlockStoragePrefix())
		settingManager := settings.NewDBManager(refManager, branchLocker, adapter, cfg.Config.GetCommittedBlockStoragePrefix())
		protectedBranchesManager = branch.NewProtectionManager(settingManager)
		stagingManager = staging.NewDBManager(cfg.DB)
		gStore = graveler.NewDBGraveler(branchLocker, committedManager, stagingManager, refManager, gcManager, protectedBranchesManager)
	}

	return &Catalog{
		BlockAdapter:  tierFSParams.Adapter,
		Store:         gStore,
		log:           logging.Default().WithField("service_name", "entry_catalog"),
		walkerFactory: cfg.WalkerFactory,
		managers:      []io.Closer{sstableManager, sstableMetaManager, &ctxCloser{cancelFn}},
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
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "name", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "storageNamespace", Value: storageNS, Fn: graveler.ValidateStorageNamespace},
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
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "name", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "storageNamespace", Value: storageNS, Fn: graveler.ValidateStorageNamespace},
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
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
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
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return err
	}
	return c.Store.DeleteRepository(ctx, repositoryID)
}

// ListRepositories list repositories information, the bool returned is true when more repositories can be listed.
// In this case pass the last repository name as 'after' on the next call to ListRepositories
func (c *Catalog) ListRepositories(ctx context.Context, limit int, prefix, after string) ([]*Repository, bool, error) {
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
	prefixRepositoryID := graveler.RepositoryID(prefix)
	startPos := prefixRepositoryID
	if afterRepositoryID > startPos {
		startPos = afterRepositoryID
	}
	if startPos != "" {
		it.SeekGE(startPos)
	}

	var repos []*Repository
	for it.Next() {
		record := it.Value()

		if !strings.HasPrefix(string(record.RepositoryID), prefix) {
			break
		}

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

func (c *Catalog) GetStagingToken(ctx context.Context, repositoryID string, branch string) (*string, error) {
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return nil, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, err
	}
	token, err := c.Store.GetStagingToken(ctx, repository, branchID)
	if err != nil {
		return nil, err
	}
	tokenString := ""
	if token != nil {
		tokenString = string(*token)
	}
	return &tokenString, nil
}

func (c *Catalog) CreateBranch(ctx context.Context, repositoryID string, branch string, sourceBranch string) (*CommitLog, error) {
	branchID := graveler.BranchID(branch)
	sourceRef := graveler.Ref(sourceBranch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
		{Name: "ref", Value: sourceRef, Fn: graveler.ValidateRef},
	}); err != nil {
		if errors.Is(err, graveler.ErrInvalidBranchID) {
			return nil, fmt.Errorf("%w: branch id must consist of letters, digits, underscores and dashes, and cannot start with a dash", err)
		}
		return nil, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, err
	}
	newBranch, err := c.Store.CreateBranch(ctx, repository, branchID, sourceRef)
	if err != nil {
		return nil, err
	}
	commit, err := c.Store.GetCommit(ctx, repository, newBranch.CommitID)
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

func (c *Catalog) DeleteBranch(ctx context.Context, repositoryID string, branch string) error {
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "name", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.DeleteBranch(ctx, repository, branchID)
}

func (c *Catalog) ListBranches(ctx context.Context, repositoryID string, prefix string, limit int, after string) ([]*Branch, bool, error) {
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return nil, false, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, false, err
	}

	// normalize limit
	if limit < 0 || limit > ListBranchesLimitMax {
		limit = ListBranchesLimitMax
	}
	it, err := c.Store.ListBranches(ctx, repository)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()

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
		b := &Branch{
			Name:      v.BranchID.String(),
			Reference: v.CommitID.String(),
		}
		branches = append(branches, b)
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

func (c *Catalog) BranchExists(ctx context.Context, repositoryID string, branch string) (bool, error) {
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "name", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return false, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return false, err
	}
	_, err = c.Store.GetBranch(ctx, repository, branchID)
	if errors.Is(err, graveler.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *Catalog) GetBranchReference(ctx context.Context, repositoryID string, branch string) (string, error) {
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return "", err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	b, err := c.Store.GetBranch(ctx, repository, branchID)
	if err != nil {
		return "", err
	}
	return string(b.CommitID), nil
}

func (c *Catalog) ResetBranch(ctx context.Context, repositoryID string, branch string) error {
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.Reset(ctx, repository, branchID)
}

func (c *Catalog) CreateTag(ctx context.Context, repositoryID string, tagID string, ref string) (string, error) {
	tag := graveler.TagID(tagID)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "tagID", Value: tag, Fn: graveler.ValidateTagID},
	}); err != nil {
		return "", err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	commitID, err := c.dereferenceCommitID(ctx, repository, graveler.Ref(ref))
	if err != nil {
		return "", err
	}
	err = c.Store.CreateTag(ctx, repository, tag, commitID)
	if err != nil {
		return "", err
	}
	return commitID.String(), nil
}

func (c *Catalog) DeleteTag(ctx context.Context, repositoryID string, tagID string) error {
	tag := graveler.TagID(tagID)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "name", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "tagID", Value: tag, Fn: graveler.ValidateTagID},
	}); err != nil {
		return err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.DeleteTag(ctx, repository, tag)
}

func (c *Catalog) ListTags(ctx context.Context, repositoryID string, prefix string, limit int, after string) ([]*Tag, bool, error) {
	if limit < 0 || limit > ListTagsLimitMax {
		limit = ListTagsLimitMax
	}
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "name", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return nil, false, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, false, err
	}
	it, err := c.Store.ListTags(ctx, repository)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	afterTagID := graveler.TagID(after)
	prefixTagID := graveler.TagID(prefix)
	if afterTagID < prefixTagID {
		it.SeekGE(prefixTagID)
	} else {
		it.SeekGE(afterTagID)
	}
	var tags []*Tag
	for it.Next() {
		v := it.Value()
		if v.TagID == afterTagID {
			continue
		}
		if !strings.HasPrefix(v.TagID.String(), prefix) {
			break
		}
		tag := &Tag{
			ID:       string(v.TagID),
			CommitID: v.CommitID.String(),
		}
		tags = append(tags, tag)
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

func (c *Catalog) GetTag(ctx context.Context, repositoryID string, tagID string) (string, error) {
	tag := graveler.TagID(tagID)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "name", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "tagID", Value: tag, Fn: graveler.ValidateTagID},
	}); err != nil {
		return "", err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	commit, err := c.Store.GetTag(ctx, repository, tag)
	if err != nil {
		return "", err
	}
	return commit.String(), nil
}

// GetEntry returns the current entry for path in repository branch reference.  Returns
// the entry with ExpiredError if it has expired from underlying storage.
func (c *Catalog) GetEntry(ctx context.Context, repositoryID string, reference string, path string, _ GetEntryParams) (*DBEntry, error) {
	refToGet := graveler.Ref(reference)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "ref", Value: refToGet, Fn: graveler.ValidateRef},
		{Name: "path", Value: Path(path), Fn: ValidatePath},
	}); err != nil {
		return nil, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, err
	}
	val, err := c.Store.Get(ctx, repository, refToGet, graveler.Key(path))
	if err != nil {
		return nil, err
	}
	ent, err := ValueToEntry(val)
	if err != nil {
		return nil, err
	}
	catalogEntry := newCatalogEntryFromEntry(false, path, ent)
	return &catalogEntry, nil
}

func newEntryFromCatalogEntry(entry DBEntry) *Entry {
	ent := &Entry{
		Address:      entry.PhysicalAddress,
		AddressType:  addressTypeToProto(entry.AddressType),
		Metadata:     entry.Metadata,
		LastModified: timestamppb.New(entry.CreationDate),
		ETag:         entry.Checksum,
		Size:         entry.Size,
		ContentType:  ContentTypeOrDefault(entry.ContentType),
	}
	return ent
}

func addressTypeToProto(t AddressType) Entry_AddressType {
	switch t {
	case AddressTypeByPrefixDeprecated:
		return Entry_BY_PREFIX_DEPRECATED
	case AddressTypeRelative:
		return Entry_RELATIVE
	case AddressTypeFull:
		return Entry_FULL
	default:
		panic(fmt.Sprintf("unknown address type: %d", t))
	}
}

func addressTypeToCatalog(t Entry_AddressType) AddressType {
	switch t {
	case Entry_BY_PREFIX_DEPRECATED:
		return AddressTypeByPrefixDeprecated
	case Entry_RELATIVE:
		return AddressTypeRelative
	case Entry_FULL:
		return AddressTypeFull
	default:
		panic(fmt.Sprintf("unknown address type: %d", t))
	}
}

func (c *Catalog) CreateEntry(ctx context.Context, repositoryID string, branch string, entry DBEntry, writeConditions ...graveler.WriteConditionOption) error {
	branchID := graveler.BranchID(branch)
	ent := newEntryFromCatalogEntry(entry)
	path := Path(entry.Path)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
		{Name: "path", Value: path, Fn: ValidatePath},
	}); err != nil {
		return err
	}
	key := graveler.Key(path)
	value, err := EntryToValue(ent)
	if err != nil {
		return err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.Set(ctx, repository, branchID, key, *value, writeConditions...)
}

func (c *Catalog) DeleteEntry(ctx context.Context, repositoryID string, branch string, path string) error {
	branchID := graveler.BranchID(branch)
	p := Path(path)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
		{Name: "path", Value: p, Fn: ValidatePath},
	}); err != nil {
		return err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	key := graveler.Key(p)
	return c.Store.Delete(ctx, repository, branchID, key)
}

func (c *Catalog) ListEntries(ctx context.Context, repositoryID string, reference string, prefix string, after string, delimiter string, limit int) ([]*DBEntry, bool, error) {
	// normalize limit
	if limit < 0 || limit > ListEntriesLimitMax {
		limit = ListEntriesLimitMax
	}
	prefixPath := Path(prefix)
	afterPath := Path(after)
	delimiterPath := Path(delimiter)
	refToList := graveler.Ref(reference)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "ref", Value: refToList, Fn: graveler.ValidateRef},
		{Name: "prefix", Value: prefixPath, Fn: ValidatePathOptional},
		{Name: "delimiter", Value: delimiterPath, Fn: ValidatePathOptional},
	}); err != nil {
		return nil, false, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, false, err
	}
	iter, err := c.Store.List(ctx, repository, refToList)
	if err != nil {
		return nil, false, err
	}
	it := NewEntryListingIterator(NewValueToEntryIterator(iter), prefixPath, delimiterPath)
	defer it.Close()

	if afterPath != "" {
		it.SeekGE(afterPath)
	}

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

func (c *Catalog) ResetEntry(ctx context.Context, repositoryID string, branch string, path string) error {
	branchID := graveler.BranchID(branch)
	entryPath := Path(path)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
		{Name: "path", Value: entryPath, Fn: ValidatePath},
	}); err != nil {
		return err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	key := graveler.Key(entryPath)
	return c.Store.ResetKey(ctx, repository, branchID, key)
}

func (c *Catalog) ResetEntries(ctx context.Context, repositoryID string, branch string, prefix string) error {
	branchID := graveler.BranchID(branch)
	prefixPath := Path(prefix)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	keyPrefix := graveler.Key(prefixPath)
	return c.Store.ResetPrefix(ctx, repository, branchID, keyPrefix)
}

func (c *Catalog) Commit(ctx context.Context, repositoryID, branch, message, committer string, metadata Metadata, date *int64, sourceMetarange *string) (*CommitLog, error) {
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return nil, err
	}

	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, err
	}

	p := graveler.CommitParams{
		Committer: committer,
		Message:   message,
		Date:      date,
		Metadata:  map[string]string(metadata),
	}
	if sourceMetarange != nil {
		x := graveler.MetaRangeID(*sourceMetarange)
		p.SourceMetaRange = &x
	}
	commitID, err := c.Store.Commit(ctx, repository, branchID, p)
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
	commit, err := c.Store.GetCommit(ctx, repository, commitID)
	if err != nil {
		return catalogCommitLog, graveler.ErrCommitNotFound
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, parent.String())
	}
	catalogCommitLog.CreationDate = commit.CreationDate.UTC()
	return catalogCommitLog, nil
}

func (c *Catalog) GetCommit(ctx context.Context, repositoryID string, reference string) (*CommitLog, error) {
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, err
	}
	commitID, err := c.dereferenceCommitID(ctx, repository, graveler.Ref(reference))
	if err != nil {
		return nil, err
	}
	commit, err := c.Store.GetCommit(ctx, repository, commitID)
	if err != nil {
		return nil, err
	}
	catalogCommitLog := &CommitLog{
		Reference:    reference,
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

func (c *Catalog) ListCommits(ctx context.Context, repositoryID string, branch string, params LogParams) ([]*CommitLog, bool, error) {
	branchRef := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchRef, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return nil, false, err
	}

	// disabling batching for this flow. See #3935 for more details
	ctx = context.WithValue(ctx, batch.SkipBatchContextKey, struct{}{})
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, false, err
	}

	commitID, err := c.dereferenceCommitID(ctx, repository, graveler.Ref(branchRef))
	if err != nil {
		return nil, false, fmt.Errorf("branch ref: %w", err)
	}
	it, err := c.Store.Log(ctx, repository, commitID)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	// skip until 'fromReference' if needed
	if params.FromReference != "" {
		fromCommitID, err := c.dereferenceCommitID(ctx, repository, graveler.Ref(params.FromReference))
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

	// commit/key to value cache - helps when fetching the same commit/key while processing parent commits
	const commitLogCacheSize = 1024 * 5
	commitCache, err := simplelru.NewLRU(commitLogCacheSize, nil)
	if err != nil {
		return nil, false, err
	}

	for it.Next() {
		v := it.Value()

		if len(params.PathList) > 0 {
			// verify that commit includes a change with object/prefix from PathList
			if len(v.Parents) != NumberOfParentsOfNonMergeCommit {
				// skip merge commits
				continue
			}
			pathInCommit, err := c.checkPathListInCommit(ctx, repository, v, params.PathList, commitCache)
			if err != nil {
				return nil, false, err
			}
			if !pathInCommit {
				// no object or prefix found skip commit
				continue
			}
		}

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
		if params.Limit && len(commits) >= params.Amount {
			break
		}
		if len(commits) >= params.Amount+1 {
			break
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	hasMore := false
	if len(commits) > params.Amount {
		hasMore = true
		commits = commits[:params.Amount]
	}
	return commits, hasMore, nil
}

// checkPathListInCommit checks whether the given commit contains changes to a list of paths.
// it searches the path in the diff between the commit, and it's parent, but do so only to commits
// that have single parent (not merge commits)
func (c *Catalog) checkPathListInCommit(ctx context.Context, repository *graveler.RepositoryRecord, commit *graveler.CommitRecord, pathList []PathRecord, commitCache *simplelru.LRU) (bool, error) {
	left := commit.Parents[0]
	right := commit.CommitID

	// diff iterator - open lazy, just in case we have a prefix to match
	var diffIter graveler.DiffIterator
	defer func() {
		if diffIter != nil {
			diffIter.Close()
		}
	}()

	// check each path
	for _, path := range pathList {
		key := graveler.Key(path.Path)
		if path.IsPrefix {
			// get diff iterator if needed for prefix lookup
			if diffIter == nil {
				var err error
				diffIter, err = c.Store.Diff(ctx, repository, graveler.Ref(left), graveler.Ref(right))
				if err != nil {
					return false, err
				}
			}
			diffIter.SeekGE(key)
			if diffIter.Next() {
				diffKey := diffIter.Value().Key
				if bytes.HasPrefix(diffKey, key) {
					return true, nil
				}
			}
			if err := diffIter.Err(); err != nil {
				return false, err
			}
		} else {
			leftObject, err := storeGetCache(ctx, c.Store, repository, left, key, commitCache)
			if err != nil {
				return false, err
			}
			rightObject, err := storeGetCache(ctx, c.Store, repository, right, key, commitCache)
			if err != nil {
				return false, err
			}
			// if left or right are missing or doesn't hold the same identify
			// we want the commit log
			if leftObject == nil && rightObject != nil ||
				leftObject != nil && rightObject == nil ||
				(leftObject != nil && rightObject != nil && !bytes.Equal(leftObject.Identity, rightObject.Identity)) {
				return true, nil
			}
		}
	}
	return false, nil
}

// storeGetCache helper to calls Get and cache the return info 'commitCache'. This method is helpful in case of calling Get
// on a large set of commits with the same object, and we can return the cached data we returned so far
func storeGetCache(ctx context.Context, store graveler.KeyValueStore, repository *graveler.RepositoryRecord, commitID graveler.CommitID, key graveler.Key, commitCache *simplelru.LRU) (*graveler.Value, error) {
	cacheKey := fmt.Sprintf("%s/%s", commitID, key)
	if o, found := commitCache.Get(cacheKey); found {
		return o.(*graveler.Value), nil
	}
	o, err := store.GetByCommitID(ctx, repository, commitID, key)
	if err != nil && !errors.Is(err, graveler.ErrNotFound) {
		return nil, err
	}
	_ = commitCache.Add(cacheKey, o)
	return o, nil
}

func (c *Catalog) Revert(ctx context.Context, repositoryID string, branch string, params RevertParams) error {
	branchID := graveler.BranchID(branch)
	reference := graveler.Ref(params.Reference)
	commitParams := graveler.CommitParams{
		Committer: params.Committer,
		Message:   fmt.Sprintf("Revert %s", params.Reference),
	}
	parentNumber := params.ParentNumber
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
		{Name: "ref", Value: reference, Fn: graveler.ValidateRef},
		{Name: "committer", Value: commitParams.Committer, Fn: validator.ValidateRequiredString},
		{Name: "message", Value: commitParams.Message, Fn: validator.ValidateRequiredString},
		{Name: "parentNumber", Value: parentNumber, Fn: validator.ValidateNonNegativeInt},
	}); err != nil {
		return err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	_, err = c.Store.Revert(ctx, repository, branchID, reference, parentNumber, commitParams)
	return err
}

func (c *Catalog) Diff(ctx context.Context, repositoryID string, leftReference string, rightReference string, params DiffParams) (Differences, bool, error) {
	left := graveler.Ref(leftReference)
	right := graveler.Ref(rightReference)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "left", Value: left, Fn: graveler.ValidateRef},
		{Name: "right", Value: right, Fn: graveler.ValidateRef},
	}); err != nil {
		return nil, false, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, false, err
	}

	iter, err := c.Store.Diff(ctx, repository, left, right)
	if err != nil {
		return nil, false, err
	}
	it := NewEntryDiffIterator(iter)
	defer it.Close()
	return listDiffHelper(it, params.Prefix, params.Delimiter, params.Limit, params.After)
}

func (c *Catalog) Compare(ctx context.Context, repositoryID, leftReference string, rightReference string, params DiffParams) (Differences, bool, error) {
	left := graveler.Ref(leftReference)
	right := graveler.Ref(rightReference)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repositoryName", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "left", Value: left, Fn: graveler.ValidateRef},
		{Name: "right", Value: right, Fn: graveler.ValidateRef},
	}); err != nil {
		return nil, false, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, false, err
	}

	iter, err := c.Store.Compare(ctx, repository, left, right)
	if err != nil {
		return nil, false, err
	}
	it := NewEntryDiffIterator(iter)
	defer it.Close()
	return listDiffHelper(it, params.Prefix, params.Delimiter, params.Limit, params.After)
}

func (c *Catalog) DiffUncommitted(ctx context.Context, repositoryID, branch, prefix, delimiter string, limit int, after string) (Differences, bool, error) {
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return nil, false, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, false, err
	}

	iter, err := c.Store.DiffUncommitted(ctx, repository, branchID)
	if err != nil {
		return nil, false, err
	}
	it := NewEntryDiffIterator(iter)
	defer it.Close()
	return listDiffHelper(it, prefix, delimiter, limit, after)
}

// GetStartPos returns a key that SeekGE will transform to a place start iterating on all elements in
//    the keys that start with 'prefix' after 'after' and taking 'delimiter' into account
func GetStartPos(prefix, after, delimiter string) string {
	if after == "" {
		// whether we have a delimiter or not, if after is not set, start at prefix
		return prefix
	}
	if after < prefix {
		// if after is before prefix, no point in starting there, start with prefix instead
		return prefix
	}
	if delimiter == "" {
		// no delimiter, continue from after
		return after
	}
	// there is a delimiter and after is not empty, start at the next common prefix after "after"
	return string(graveler.UpperBoundForPrefix([]byte(after)))
}

const commonPrefixSplitParts = 2

func listDiffHelper(it EntryDiffIterator, prefix, delimiter string, limit int, after string) (Differences, bool, error) {
	if limit < 0 || limit > DiffLimitMax {
		limit = DiffLimitMax
	}
	seekStart := GetStartPos(prefix, after, delimiter)
	it.SeekGE(Path(seekStart))

	diffs := make(Differences, 0)
	for it.Next() {
		v := it.Value()
		path := string(v.Path)

		if path == after {
			continue // emulate SeekGT using SeekGE
		}
		if !strings.HasPrefix(path, prefix) {
			break // we only want things that start with prefix, apparently there are none left
		}

		if delimiter != "" {
			// common prefix logic goes here.
			// for every path, after trimming "prefix", take the string upto-and-including the delimiter
			// if the received path == the entire remainder, add that object as is
			// if it's just a part of the name, add it as a "common prefix" entry -
			//   and skip to next record following all those starting with this prefix
			pathRelativeToPrefix := strings.TrimPrefix(path, prefix)
			// we want the common prefix and the remainder
			parts := strings.SplitN(pathRelativeToPrefix, delimiter, commonPrefixSplitParts)
			if len(parts) == commonPrefixSplitParts {
				// a common prefix exists!
				commonPrefix := prefix + parts[0] + delimiter
				diffs = append(diffs, Difference{
					DBEntry: NewDBEntryBuilder().CommonLevel(true).Path(commonPrefix).Build(),
					// We always return "changed" for common prefixes. Seeing if a common prefix is e.g. deleted is O(N)
					Type: DifferenceTypeChanged,
				})
				if len(diffs) >= limit+1 {
					break // collected enough results
				}

				// let's keep collecting records. We want the next record that doesn't
				//   start with this common prefix
				it.SeekGE(Path(graveler.UpperBoundForPrefix([]byte(commonPrefix))))
				continue
			}
		}

		// got a regular entry
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

func (c *Catalog) Merge(ctx context.Context, repositoryID string, destinationBranch string, sourceRef string,
	committer string, message string, metadata Metadata, strategy string,
) (string, error) {
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
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "destination", Value: destination, Fn: graveler.ValidateBranchID},
		{Name: "source", Value: source, Fn: graveler.ValidateRef},
		{Name: "committer", Value: commitParams.Committer, Fn: validator.ValidateRequiredString},
		{Name: "message", Value: commitParams.Message, Fn: validator.ValidateRequiredString},
		{Name: "strategy", Value: strategy, Fn: graveler.ValidateRequiredStrategy},
	}); err != nil {
		return "", err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}

	commitID, err := c.Store.Merge(ctx, repository, destination, source, commitParams, strategy)
	if errors.Is(err, graveler.ErrConflictFound) {
		// for compatibility with old Catalog
		return "", err
	}
	if err != nil {
		return "", err
	}
	return commitID.String(), nil
}

func (c *Catalog) DumpCommits(ctx context.Context, repositoryID string) (string, error) {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}

	metaRangeID, err := c.Store.DumpCommits(ctx, repository)
	if err != nil {
		return "", err
	}
	return string(*metaRangeID), nil
}

func (c *Catalog) DumpBranches(ctx context.Context, repositoryID string) (string, error) {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}

	metaRangeID, err := c.Store.DumpBranches(ctx, repository)
	if err != nil {
		return "", err
	}
	return string(*metaRangeID), nil
}

func (c *Catalog) DumpTags(ctx context.Context, repositoryID string) (string, error) {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}

	metaRangeID, err := c.Store.DumpTags(ctx, repository)
	if err != nil {
		return "", err
	}
	return string(*metaRangeID), nil
}

func (c *Catalog) LoadCommits(ctx context.Context, repositoryID, commitsMetaRangeID string) error {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.LoadCommits(ctx, repository, graveler.MetaRangeID(commitsMetaRangeID))
}

func (c *Catalog) LoadBranches(ctx context.Context, repositoryID, branchesMetaRangeID string) error {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.LoadBranches(ctx, repository, graveler.MetaRangeID(branchesMetaRangeID))
}

func (c *Catalog) LoadTags(ctx context.Context, repositoryID, tagsMetaRangeID string) error {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.LoadTags(ctx, repository, graveler.MetaRangeID(tagsMetaRangeID))
}

func (c *Catalog) GetMetaRange(ctx context.Context, repositoryID, metaRangeID string) (graveler.MetaRangeAddress, error) {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	return c.Store.GetMetaRange(ctx, repository, graveler.MetaRangeID(metaRangeID))
}

func (c *Catalog) GetRange(ctx context.Context, repositoryID, rangeID string) (graveler.RangeAddress, error) {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return "", err
	}
	return c.Store.GetRange(ctx, repository, graveler.RangeID(rangeID))
}

func (c *Catalog) WriteRange(ctx context.Context, repositoryID, fromSourceURI, prepend, after, continuationToken string) (*graveler.RangeInfo, *Mark, error) {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, nil, err
	}

	walker, err := c.walkerFactory.GetWalker(ctx, store.WalkerOptions{StorageURI: fromSourceURI})
	if err != nil {
		return nil, nil, fmt.Errorf("creating object-store walker: %w", err)
	}

	it, err := NewWalkEntryIterator(ctx, walker, prepend, after, continuationToken)
	if err != nil {
		return nil, nil, fmt.Errorf("creating walk iterator: %w", err)
	}
	defer it.Close()

	rangeInfo, err := c.Store.WriteRange(ctx, repository, NewEntryToValueIterator(it))
	if err != nil {
		return nil, nil, fmt.Errorf("writing range from entry iterator: %w", err)
	}
	mark := it.Marker()

	return rangeInfo, &mark, nil
}

func (c *Catalog) WriteMetaRange(ctx context.Context, repositoryID string, ranges []*graveler.RangeInfo) (*graveler.MetaRangeInfo, error) {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, err
	}
	return c.Store.WriteMetaRange(ctx, repository, ranges)
}

func (c *Catalog) GetGarbageCollectionRules(ctx context.Context, repositoryID string) (*graveler.GarbageCollectionRules, error) {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, err
	}
	return c.Store.GetGarbageCollectionRules(ctx, repository)
}

func (c *Catalog) SetGarbageCollectionRules(ctx context.Context, repositoryID string, rules *graveler.GarbageCollectionRules) error {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.SetGarbageCollectionRules(ctx, repository, rules)
}

func (c *Catalog) GetBranchProtectionRules(ctx context.Context, repositoryID string) (*graveler.BranchProtectionRules, error) {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, err
	}

	return c.Store.GetBranchProtectionRules(ctx, repository)
}

func (c *Catalog) DeleteBranchProtectionRule(ctx context.Context, repositoryID string, pattern string) error {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.DeleteBranchProtectionRule(ctx, repository, pattern)
}

func (c *Catalog) CreateBranchProtectionRule(ctx context.Context, repositoryID string, pattern string, blockedActions []graveler.BranchProtectionBlockedAction) error {
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return err
	}
	return c.Store.CreateBranchProtectionRule(ctx, repository, pattern, blockedActions)
}

func (c *Catalog) PrepareExpiredCommits(ctx context.Context, repositoryID string, previousRunID string) (*graveler.GarbageCollectionRunMetadata, error) {
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	repository, err := c.Store.GetRepository(ctx, graveler.RepositoryID(repositoryID))
	if err != nil {
		return nil, err
	}
	return c.Store.SaveGarbageCollectionCommits(ctx, repository, previousRunID)
}

func (c *Catalog) Close() error {
	var errs error
	for _, manager := range c.managers {
		err := manager.Close()
		if err != nil {
			_ = multierror.Append(errs, err)
		}
	}
	return errs
}

// dereferenceCommitID dereference 'ref' to a commit ID, this helper makes sure we do not point to explicit branch staging
func (c *Catalog) dereferenceCommitID(ctx context.Context, repository *graveler.RepositoryRecord, ref graveler.Ref) (graveler.CommitID, error) {
	resolvedRef, err := c.Store.Dereference(ctx, repository, ref)
	if err != nil {
		return "", err
	}
	if resolvedRef.CommitID == "" {
		return "", fmt.Errorf("%w: no commit", ErrInvalidRef)
	}
	if resolvedRef.ResolvedBranchModifier == graveler.ResolvedBranchModifierStaging {
		return "", fmt.Errorf("%w: should point to a commit", ErrInvalidRef)
	}
	return resolvedRef.CommitID, nil
}

func newCatalogEntryFromEntry(commonPrefix bool, path string, ent *Entry) DBEntry {
	b := NewDBEntryBuilder().
		CommonLevel(commonPrefix).
		Path(path)
	if ent != nil {
		b.PhysicalAddress(ent.Address)
		b.AddressType(addressTypeToCatalog(ent.AddressType))
		b.CreationDate(ent.LastModified.AsTime())
		b.Size(ent.Size)
		b.Checksum(ent.ETag)
		b.Metadata(ent.Metadata)
		b.Expired(false)
		b.AddressType(addressTypeToCatalog(ent.AddressType))
		b.ContentType(ContentTypeOrDefault(ent.ContentType))
	}
	return b.Build()
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
