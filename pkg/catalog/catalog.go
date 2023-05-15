package catalog

import (
	"bytes"
	"container/heap"
	"context"
	"crypto"
	_ "crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/alitto/pond"
	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-multierror"
	lru "github.com/hnlq715/golang-lru"
	"github.com/rs/xid"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/config"
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
	"github.com/treeverse/lakefs/pkg/upload"
	"github.com/treeverse/lakefs/pkg/validator"
	"go.uber.org/atomic"
	"go.uber.org/ratelimit"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// hashAlg is the hashing algorithm to use to generate graveler identifiers.  Changing it
	// causes all old identifiers to change, so while existing installations will continue to
	// function they will be unable to re-use any existing objects.
	hashAlg = crypto.SHA256

	NumberOfParentsOfNonMergeCommit = 1

	gcParquetParallelNum            = 1                // Number of goroutines to handle marshaling of data
	defaultGCMaxUncommittedFileSize = 20 * 1024 * 1024 // 20 MB

	// Calculation of size deviation by gcPeriodicCheckSize value
	// "data" prefix = 4 bytes
	// partition id = 20 bytes (xid)
	// object name = 20 bytes (xid)
	// timestamp (int64) = 8 bytes
	//
	// Total per entry ~52 bytes
	// Deviation with gcPeriodicCheckSize = 100000 will be around 5 MB
	gcPeriodicCheckSize = 100000
)

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
	Config                   *config.Config
	KVStore                  kv.Store
	WalkerFactory            WalkerFactory
	SettingsManagerOption    settings.ManagerOption
	GCMaxUncommittedFileSize int // The maximum file size for uncommitted dump created during PrepareUncommittedGC
	PathProvider             *upload.PathPartitionProvider
	Limiter                  ratelimit.Limiter
}

type Catalog struct {
	BlockAdapter             block.Adapter
	Store                    Store
	GCMaxUncommittedFileSize int
	log                      logging.Logger
	walkerFactory            WalkerFactory
	managers                 []io.Closer
	workPool                 *pond.WorkerPool
	PathProvider             *upload.PathPartitionProvider
	BackgroundLimiter        ratelimit.Limiter
	KVStore                  kv.Store
	KVStoreLimited           kv.Store
	addressProvider          *ident.HexAddressProvider
}

const (
	ListRepositoriesLimitMax = 1000
	ListBranchesLimitMax     = 1000
	ListTagsLimitMax         = 1000
	DiffLimitMax             = 1000
	ListEntriesLimitMax      = 10000
	sharedWorkers            = 30
	pendingTasksPerWorker    = 3
	workersMaxDrainDuration  = 5 * time.Second
)

type ImportPathType string

const (
	ImportPathTypePrefix = "common_prefix"
	ImportPathTypeObject = "object"
)

type ImportPath struct {
	Path        string
	Destination string
	Type        ImportPathType
}

func GetImportPathType(t string) (ImportPathType, error) {
	switch t {
	case ImportPathTypePrefix,
		ImportPathTypeObject:
		return ImportPathType(t), nil
	default:
		return "", fmt.Errorf("invalid import type: %w", graveler.ErrInvalidValue)
	}
}

type ImportCommit struct {
	CommitMessage string
	Committer     string
	Metadata      Metadata
}

type ImportRequest struct {
	Paths  []ImportPath
	Commit ImportCommit
}

type ctxCloser struct {
	close context.CancelFunc
}

func (c *ctxCloser) Close() error {
	go c.close()
	return nil
}

func New(ctx context.Context, cfg Config) (*Catalog, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	adapter, err := factory.BuildBlockAdapter(ctx, nil, cfg.Config)
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("build block adapter: %w", err)
	}
	if cfg.WalkerFactory == nil {
		cfg.WalkerFactory = store.NewFactory(cfg.Config)
	}
	if cfg.GCMaxUncommittedFileSize == 0 {
		cfg.GCMaxUncommittedFileSize = defaultGCMaxUncommittedFileSize
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

	committedParams := cfg.Config.CommittedParams()
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

	storeLimiter := kv.NewStoreLimiter(cfg.KVStore, cfg.Limiter)
	addressProvider := ident.NewHexAddressProvider()
	refManager := ref.NewRefManager(
		ref.ManagerConfig{
			Executor:              executor,
			KVStore:               cfg.KVStore,
			KVStoreLimited:        storeLimiter,
			AddressProvider:       addressProvider,
			RepositoryCacheConfig: ref.CacheConfig(cfg.Config.Graveler.RepositoryCache),
			CommitCacheConfig:     ref.CacheConfig(cfg.Config.Graveler.CommitCache),
		})
	gcManager := retention.NewGarbageCollectionManager(tierFSParams.Adapter, refManager, cfg.Config.Committed.BlockStoragePrefix)
	settingManager := settings.NewManager(refManager, cfg.KVStore)
	if cfg.SettingsManagerOption != nil {
		cfg.SettingsManagerOption(settingManager)
	}

	protectedBranchesManager := branch.NewProtectionManager(settingManager)
	stagingManager := staging.NewManager(ctx, cfg.KVStore, storeLimiter)
	gStore := graveler.NewGraveler(committedManager, stagingManager, refManager, gcManager, protectedBranchesManager)

	// The size of the workPool is determined by the number of workers and the number of desired pending tasks for each worker.
	workPool := pond.New(sharedWorkers, sharedWorkers*pendingTasksPerWorker, pond.Context(ctx))
	return &Catalog{
		BlockAdapter:             tierFSParams.Adapter,
		Store:                    gStore,
		GCMaxUncommittedFileSize: cfg.GCMaxUncommittedFileSize,
		PathProvider:             cfg.PathProvider,
		BackgroundLimiter:        cfg.Limiter,
		log:                      logging.Default().WithField("service_name", "entry_catalog"),
		walkerFactory:            cfg.WalkerFactory,
		workPool:                 workPool,
		KVStore:                  cfg.KVStore,
		managers:                 []io.Closer{sstableManager, sstableMetaManager, &ctxCloser{cancelFn}},
		KVStoreLimited:           storeLimiter,
		addressProvider:          addressProvider,
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

func (c *Catalog) getRepository(ctx context.Context, repository string) (*graveler.RepositoryRecord, error) {
	repositoryID := graveler.RepositoryID(repository)
	return c.Store.GetRepository(ctx, repositoryID)
}

// GetRepository get repository information
func (c *Catalog) GetRepository(ctx context.Context, repository string) (*Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	repo, err := c.getRepository(ctx, repository)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	if err := c.checkCommitIDDuplication(ctx, repository, graveler.CommitID(branchID)); err != nil {
		return nil, err
	}

	if _, err := c.Store.GetTag(ctx, repository, graveler.TagID(branchID)); err == nil {
		return nil, fmt.Errorf("tag ID %s: %w", branchID, graveler.ErrConflictFound)
	} else if !errors.Is(err, graveler.ErrNotFound) {
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}
	if _, err := c.Store.GetBranch(ctx, repository, graveler.BranchID(tagID)); err == nil {
		return "", fmt.Errorf("branch name %s: %w", tagID, graveler.ErrConflictFound)
	} else if !errors.Is(err, graveler.ErrNotFound) {
		return "", err
	}
	if err := c.checkCommitIDDuplication(ctx, repository, graveler.CommitID(tagID)); err != nil {
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
func (c *Catalog) GetEntry(ctx context.Context, repositoryID string, reference string, path string, params GetEntryParams) (*DBEntry, error) {
	refToGet := graveler.Ref(reference)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "ref", Value: refToGet, Fn: graveler.ValidateRef},
		{Name: "path", Value: Path(path), Fn: ValidatePath},
	}); err != nil {
		return nil, err
	}
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	val, err := c.Store.Get(ctx, repository, refToGet, graveler.Key(path), graveler.WithStageOnly(params.StageOnly))
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

func (c *Catalog) CreateEntry(ctx context.Context, repositoryID string, branch string, entry DBEntry, opts ...graveler.SetOptionsFunc) error {
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
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.Set(ctx, repository, branchID, key, *value, opts...)
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
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	key := graveler.Key(p)
	return c.Store.Delete(ctx, repository, branchID, key)
}

func (c *Catalog) DeleteEntries(ctx context.Context, repositoryID string, branch string, paths []string) error {
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return err
	}
	// validate path
	for i, path := range paths {
		p := Path(path)
		if err := ValidatePath(p); err != nil {
			return fmt.Errorf("argument path[%d]: %w", i, err)
		}
	}

	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}

	keys := make([]graveler.Key, len(paths))
	for i := range paths {
		keys[i] = graveler.Key(paths[i])
	}
	return c.Store.DeleteBatch(ctx, repository, branchID, keys)
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
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, false, err
	}
	iter, err := c.Store.List(ctx, repository, refToList, limit+1)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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

	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
		Reference:    commitID.String(),
		Committer:    commit.Committer,
		Message:      commit.Message,
		CreationDate: commit.CreationDate,
		MetaRangeID:  string(commit.MetaRangeID),
		Metadata:     Metadata(commit.Metadata),
		Parents:      []string{},
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
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, false, err
	}

	commitID, err := c.dereferenceCommitID(ctx, repository, graveler.Ref(branchRef))
	if err != nil {
		return nil, false, fmt.Errorf("branch ref: %w", err)
	}
	it, err := c.Store.Log(ctx, repository, commitID, params.FirstParent)
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

	paths := params.PathList
	if len(paths) == 0 {
		return listCommitsWithoutPaths(it, params)
	}

	return c.listCommitsWithPaths(ctx, repository, it, params)
}

func (c *Catalog) listCommitsWithPaths(ctx context.Context, repository *graveler.RepositoryRecord, it graveler.CommitIterator, params LogParams) ([]*CommitLog, bool, error) {
	// verify we are not listing commits without any paths
	if len(params.PathList) == 0 {
		return nil, false, fmt.Errorf("%w: list commits without paths", graveler.ErrInvalid)
	}

	// commit/key to value cache - helps when fetching the same commit/key while processing parent commits
	const commitLogCacheSize = 1024 * 5
	commitCache, err := lru.New(commitLogCacheSize)
	if err != nil {
		return nil, false, err
	}

	const numReadResults = 3
	done := atomic.NewBool(false)

	// Shared workPool for the workers. 2 designated to create the work and receive the result
	paths := params.PathList

	// iterate over commits log and push work into work channel
	outCh := make(chan *commitLogJob, numReadResults)
	var mgmtGroup multierror.Group
	mgmtGroup.Go(func() error {
		defer close(outCh)

		// workers to check if commit record in path
		workerGroup, ctx := c.workPool.GroupContext(ctx)

		current := 0
	readLoop:
		for it.Next() && !done.Load() {
			// if context canceled we stop processing
			select {
			case <-ctx.Done():
				break readLoop
			default:
			}

			commitRecord := it.Value()
			// skip merge commits
			if len(commitRecord.Parents) != NumberOfParentsOfNonMergeCommit {
				continue
			}

			// submit work to the pool
			commitOrder := current
			current++
			workerGroup.Submit(func() error {
				pathInCommit, err := c.checkPathListInCommit(ctx, repository, commitRecord, paths, commitCache)
				if err != nil {
					return err
				}
				job := &commitLogJob{order: commitOrder}
				if pathInCommit {
					job.log = CommitRecordToLog(commitRecord)
				}
				outCh <- job
				return nil
			})
		}
		// wait until workers are done and return the first error
		if err := workerGroup.Wait(); err != nil {
			return err
		}
		return it.Err()
	})

	// process out channel to keep order into results channel by using heap
	resultCh := make(chan *CommitLog, numReadResults)
	var jobsHeap commitLogJobHeap
	mgmtGroup.Go(func() error {
		defer close(resultCh)
		// read and sort by heap the result to results channel
		current := 0
		for result := range outCh {
			heap.Push(&jobsHeap, result)
			for len(jobsHeap) > 0 && jobsHeap[0].order == current {
				job := heap.Pop(&jobsHeap).(*commitLogJob)
				if job.log != nil {
					resultCh <- job.log
				}
				current++
			}
		}
		// flush heap content when no more results on output channel
		for len(jobsHeap) > 0 {
			job := heap.Pop(&jobsHeap).(*commitLogJob)
			if job.log != nil {
				resultCh <- job.log
			}
		}
		return nil
	})

	// fill enough results, in case of an error the result channel will be closed
	commits := make([]*CommitLog, 0)
	for res := range resultCh {
		commits = append(commits, res)
		if foundAllCommits(params, commits) {
			// All results returned until the last commit found
			// and the number of commits found is as expected.
			// we have what we need.
			break
		}
	}
	// mark we stopped processing results and throw if needed all the rest
	done.Store(true)
	for range resultCh {
		// drain results channel
	}

	// wait until background work is completed
	if err := mgmtGroup.Wait().ErrorOrNil(); err != nil {
		return nil, false, err
	}
	return logCommitsResult(commits, params)
}

func listCommitsWithoutPaths(it graveler.CommitIterator, params LogParams) ([]*CommitLog, bool, error) {
	// no need to parallelize here - just read the commits
	var commits []*CommitLog
	for it.Next() {
		val := it.Value()

		commits = append(commits, CommitRecordToLog(val))
		if foundAllCommits(params, commits) {
			// All results returned until the last commit found
			// and the number of commits found is as expected.
			// we have what we need. return commits, true
			break
		}
	}
	if it.Err() != nil {
		return nil, false, it.Err()
	}

	return logCommitsResult(commits, params)
}

func foundAllCommits(params LogParams, commits []*CommitLog) bool {
	return (params.Limit && len(commits) >= params.Amount) ||
		len(commits) >= params.Amount+1
}

func commitLogToRecord(val *CommitLog) *graveler.CommitRecord {
	record := &graveler.CommitRecord{
		CommitID: graveler.CommitID(val.Reference),
		Commit: &graveler.Commit{
			Committer:    val.Committer,
			Message:      val.Message,
			MetaRangeID:  graveler.MetaRangeID(val.MetaRangeID),
			CreationDate: val.CreationDate,
			Metadata:     map[string]string(val.Metadata),
		},
	}
	for _, parent := range val.Parents {
		record.Parents = append(record.Parents, graveler.CommitID(parent))
	}
	return record
}

func CommitRecordToLog(val *graveler.CommitRecord) *CommitLog {
	if val == nil {
		return nil
	}
	commit := &CommitLog{
		Reference:    val.CommitID.String(),
		Committer:    val.Committer,
		Message:      val.Message,
		CreationDate: val.CreationDate,
		Metadata:     map[string]string(val.Metadata),
		MetaRangeID:  string(val.MetaRangeID),
		Parents:      make([]string, 0, len(val.Parents)),
	}
	for _, parent := range val.Parents {
		commit.Parents = append(commit.Parents, parent.String())
	}
	return commit
}

func logCommitsResult(commits []*CommitLog, params LogParams) ([]*CommitLog, bool, error) {
	hasMore := false
	if len(commits) > params.Amount {
		hasMore = true
		commits = commits[:params.Amount]
	}
	return commits, hasMore, nil
}

type commitLogJob struct {
	order int
	log   *CommitLog
}

// commitLogJobHeap heap of commit logs based on order. The minimum element in the tree is the root, at index 0.
type commitLogJobHeap []*commitLogJob

//goland:noinspection GoMixedReceiverTypes
func (h commitLogJobHeap) Len() int { return len(h) }

//goland:noinspection GoMixedReceiverTypes
func (h commitLogJobHeap) Less(i, j int) bool { return h[i].order < h[j].order }

//goland:noinspection GoMixedReceiverTypes
func (h commitLogJobHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

//goland:noinspection GoMixedReceiverTypes
func (h *commitLogJobHeap) Push(x interface{}) {
	*h = append(*h, x.(*commitLogJob))
}

//goland:noinspection GoMixedReceiverTypes
func (h *commitLogJobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// checkPathListInCommit checks whether the given commit contains changes to a list of paths.
// it searches the path in the diff between the commit, and it's parent, but do so only to commits
// that have single parent (not merge commits)
func (c *Catalog) checkPathListInCommit(ctx context.Context, repository *graveler.RepositoryRecord, commit *graveler.CommitRecord, pathList []PathRecord, commitCache *lru.Cache) (bool, error) {
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
			// check if the key exists in both commits
			// First, check if we can compare the ranges.
			// If ranges match, or if no ranges for both commits, we can skip the key lookup.
			lRangeID, err := c.Store.GetRangeIDByKey(ctx, repository, left, key)
			lFound := !errors.Is(err, graveler.ErrNotFound)
			if err != nil && lFound {
				return false, err
			}
			rRangeID, err := c.Store.GetRangeIDByKey(ctx, repository, right, key)
			rFound := !errors.Is(err, graveler.ErrNotFound)
			if err != nil && rFound {
				return false, err
			}

			if !lFound && !rFound {
				// no range matching the key exist in both commits
				continue
			}
			if lRangeID == rRangeID {
				// It's the same range - the value of the key is identical in both
				continue
			}

			// The key possibly exists in both commits, but the range ID is different - the value is needs to be looked at
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
func storeGetCache(ctx context.Context, store graveler.KeyValueStore, repository *graveler.RepositoryRecord, commitID graveler.CommitID, key graveler.Key, commitCache *lru.Cache) (*graveler.Value, error) {
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
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	_, err = c.Store.Revert(ctx, repository, branchID, reference, parentNumber, commitParams)
	return err
}

func (c *Catalog) CherryPick(ctx context.Context, repositoryID string, branch string, params CherryPickParams) (*CommitLog, error) {
	branchID := graveler.BranchID(branch)
	reference := graveler.Ref(params.Reference)
	parentNumber := params.ParentNumber
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
		{Name: "ref", Value: reference, Fn: graveler.ValidateRef},
		{Name: "committer", Value: params.Committer, Fn: validator.ValidateRequiredString},
		{Name: "parentNumber", Value: parentNumber, Fn: validator.ValidateNilOrPositiveInt},
	}); err != nil {
		return nil, err
	}
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}

	commitID, err := c.Store.CherryPick(ctx, repository, branchID, reference, parentNumber, params.Committer)
	if err != nil {
		return nil, err
	}

	// in order to return commit log we need the commit creation time and parents
	commit, err := c.Store.GetCommit(ctx, repository, commitID)
	if err != nil {
		return nil, graveler.ErrCommitNotFound
	}

	catalogCommitLog := &CommitLog{
		Reference:    commitID.String(),
		Committer:    params.Committer,
		Message:      commit.Message,
		CreationDate: commit.CreationDate.UTC(),
		MetaRangeID:  string(commit.MetaRangeID),
		Metadata:     Metadata(commit.Metadata),
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, parent.String())
	}
	return catalogCommitLog, nil
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
//
//	the keys that start with 'prefix' after 'after' and taking 'delimiter' into account
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
					Type: DifferenceTypePrefixChanged,
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

func (c *Catalog) Merge(ctx context.Context, repositoryID string, destinationBranch string, sourceRef string, committer string, message string, metadata Metadata, strategy string) (string, error) {
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

	// disabling batching for this flow. See #3935 for more details
	ctx = context.WithValue(ctx, batch.SkipBatchContextKey, struct{}{})

	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}

	commitID, err := c.Store.Merge(ctx, repository, destination, source, commitParams, strategy)
	if err != nil {
		return "", err
	}
	return commitID.String(), nil
}

func (c *Catalog) FindMergeBase(ctx context.Context, repositoryID string, destinationRef string, sourceRef string) (string, string, string, error) {
	destination := graveler.Ref(destinationRef)
	source := graveler.Ref(sourceRef)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "destination", Value: destination, Fn: graveler.ValidateRef},
		{Name: "source", Value: source, Fn: graveler.ValidateRef},
	}); err != nil {
		return "", "", "", err
	}

	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return "", "", "", err
	}

	fromCommit, toCommit, baseCommit, err := c.Store.FindMergeBase(ctx, repository, destination, source)
	if err != nil {
		return "", "", "", err
	}
	return fromCommit.CommitID.String(), toCommit.CommitID.String(), c.addressProvider.ContentAddress(baseCommit), nil
}

func (c *Catalog) DumpCommits(ctx context.Context, repositoryID string) (string, error) {
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.LoadCommits(ctx, repository, graveler.MetaRangeID(commitsMetaRangeID))
}

func (c *Catalog) LoadBranches(ctx context.Context, repositoryID, branchesMetaRangeID string) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.LoadBranches(ctx, repository, graveler.MetaRangeID(branchesMetaRangeID))
}

func (c *Catalog) LoadTags(ctx context.Context, repositoryID, tagsMetaRangeID string) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.LoadTags(ctx, repository, graveler.MetaRangeID(tagsMetaRangeID))
}

func (c *Catalog) GetMetaRange(ctx context.Context, repositoryID, metaRangeID string) (graveler.MetaRangeAddress, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}
	return c.Store.GetMetaRange(ctx, repository, graveler.MetaRangeID(metaRangeID))
}

func (c *Catalog) GetRange(ctx context.Context, repositoryID, rangeID string) (graveler.RangeAddress, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}
	return c.Store.GetRange(ctx, repository, graveler.RangeID(rangeID))
}

func (c *Catalog) ensureBranchExists(ctx context.Context, repository *graveler.RepositoryRecord, branch, sourceBranch string) error {
	branchID := graveler.BranchID(branch)
	_, err := c.Store.GetBranch(ctx, repository, branchID)
	if errors.Is(err, graveler.ErrNotFound) {
		_, err = c.Store.CreateBranch(ctx, repository, branchID, graveler.Ref(sourceBranch))
	}
	return err
}

func (c *Catalog) importAsync(repository *graveler.RepositoryRecord, branchID, importID string, params ImportRequest) {
	ctx, cancel := context.WithCancel(context.Background()) // Need a new context for the async operations
	defer cancel()
	logger := c.log.WithField("import_id", importID)
	importManager, err := NewImport(ctx, cancel, logger, c.KVStore, repository, importID)
	if err != nil {
		logger.WithError(err).Error("creating import manager")
		return
	}
	defer importManager.Close()

	importStatus := importManager.Status()
	importBranch := "_" + branchID + "_imported"
	err = c.ensureBranchExists(ctx, repository, importBranch, branchID)
	if err != nil {
		importStatus.Error = fmt.Errorf("ensure import branch: %w", err)
		importManager.StatusChan <- importStatus
		return
	}
	importStatus.ImportBranch = importBranch

	wg, wgCtx := c.workPool.GroupContext(ctx)
	for _, source := range params.Paths {
		// TODO (niro): Need to handle this at some point (use adapter GetWalker)
		walker, err := c.walkerFactory.GetWalker(wgCtx, store.WalkerOptions{StorageURI: source.Path})
		if err != nil {
			importStatus.Error = fmt.Errorf("creating object-store walker on path %s: %w", source.Path, err)
			importManager.StatusChan <- importStatus
			return
		}

		it, err := NewWalkEntryIterator(wgCtx, walker, source.Type, source.Destination, "", "")
		if err != nil {
			importStatus.Error = fmt.Errorf("creating walk iterator on path %s: %w", source.Path, err)
			importManager.StatusChan <- importStatus
			return
		}
		logger.WithFields(logging.Fields{"source": source.Path, "itr": it}).Debug("Ingest source")

		wg.Submit(func() error {
			defer it.Close()
			return importManager.Ingest(it)
		})

		// Check if operation was canceled
		if ctx.Err() != nil {
			return
		}
	}

	err = wg.Wait()
	if err != nil {
		importStatus.Error = fmt.Errorf("error on ingest: %w", err)
		importManager.StatusChan <- importStatus
		return
	}

	importItr, err := importManager.NewItr()
	if err != nil {
		importStatus.Error = fmt.Errorf("error on import iterator: %w", err)
		importManager.StatusChan <- importStatus
		return
	}
	defer importItr.Close()

	var ranges []*graveler.RangeInfo
	for importItr.hasMore {
		rangeInfo, err := c.Store.WriteRange(ctx, repository, importItr)
		if err != nil {
			importStatus.Error = fmt.Errorf("write range: %w", err)
			importManager.StatusChan <- importStatus
			return
		}

		ranges = append(ranges, rangeInfo)
		// Check if operation was canceled
		if ctx.Err() != nil {
			return
		}
	}

	// Create metarange
	metarange, err := c.Store.WriteMetaRange(ctx, repository, ranges)
	if err != nil {
		importStatus.Error = fmt.Errorf("create metarange: %w", err)
		importManager.StatusChan <- importStatus
		return
	}

	// Commit the changes
	metarangeStr := metarange.ID.String()
	cParams := params.Commit
	commit, err := c.Commit(ctx, repository.RepositoryID.String(), importBranch, cParams.CommitMessage, cParams.Committer, cParams.Metadata, nil, &metarangeStr)
	if err != nil {
		importStatus.Error = fmt.Errorf("commit changes: %w", err)
		importManager.StatusChan <- importStatus
		return
	}

	// Update import status
	importStatus.MetaRangeID = metarange.ID
	importStatus.Commit = commitLogToRecord(commit)
	importStatus.Completed = true
	importManager.StatusChan <- importStatus
}

func (c *Catalog) Import(ctx context.Context, repositoryID, branchID string, params ImportRequest) (string, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}

	_, err = c.Store.GetBranch(ctx, repository, graveler.BranchID(branchID))
	if err != nil {
		return "", err
	}

	id := xid.New().String()
	// Run import
	go func() {
		c.importAsync(repository, branchID, id, params)
	}()
	return id, nil
}

func (c *Catalog) CancelImport(ctx context.Context, repositoryID, importID string) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}

	importStatus, err := c.getImportStatus(ctx, repository, importID)
	if err != nil {
		return err
	}
	if importStatus.Completed || importStatus.Error == ImportCanceled {
		c.log.WithFields(logging.Fields{
			"import_id": importID,
			"completed": importStatus.Completed,
			"error":     importStatus.Error,
		}).Warning("Not canceling import")
		return graveler.ErrConflictFound
	}
	importStatus.Error = ImportCanceled
	importStatus.UpdatedAt = timestamppb.Now()
	return kv.SetMsg(ctx, c.KVStore, graveler.RepoPartition(repository), []byte(importID), importStatus)
}

func (c *Catalog) getImportStatus(ctx context.Context, repository *graveler.RepositoryRecord, importID string) (*graveler.ImportStatusData, error) {
	repoPartition := graveler.RepoPartition(repository)
	data := &graveler.ImportStatusData{}
	_, err := kv.GetMsg(ctx, c.KVStore, repoPartition, []byte(importID), data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return nil, graveler.ErrNotFound
		}
		return nil, err
	}
	return data, nil
}

func (c *Catalog) GetImportStatus(ctx context.Context, repositoryID, importID string) (*graveler.ImportStatus, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}

	data, err := c.getImportStatus(ctx, repository, importID)
	if err != nil {
		return nil, err
	}
	return graveler.ImportStatusFromProto(data), nil
}

func (c *Catalog) WriteRange(ctx context.Context, repositoryID string, params WriteRangeRequest) (*graveler.RangeInfo, *Mark, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, nil, err
	}

	// TODO (niro): Need to handle this at some point (use adapter GetWalker)
	walker, err := c.walkerFactory.GetWalker(ctx, store.WalkerOptions{StorageURI: params.SourceURI, SkipOutOfOrder: true})
	if err != nil {
		return nil, nil, fmt.Errorf("creating object-store walker: %w", err)
	}

	it, err := NewWalkEntryIterator(ctx, walker, ImportPathTypePrefix, params.Prepend, params.After, params.ContinuationToken)
	if err != nil {
		return nil, nil, fmt.Errorf("creating walk iterator: %w", err)
	}
	defer it.Close()

	rangeInfo, err := c.Store.WriteRange(ctx, repository, NewEntryToValueIterator(it))
	if err != nil {
		return nil, nil, fmt.Errorf("writing range from entry iterator: %w", err)
	}

	stagingToken := params.StagingToken
	skipped := it.GetSkippedEntries()
	if len(skipped) > 0 {
		c.log.Warning("Skipped count:", len(skipped))
		if stagingToken == "" {
			stagingToken = graveler.GenerateStagingToken("import", "ingest_range").String()
		}

		for _, obj := range skipped {
			p := params.Prepend + obj.RelativeKey
			entryRecord := objectStoreEntryToEntryRecord(obj, p)
			entry, err := EntryToValue(entryRecord.Entry)
			if err != nil {
				return nil, nil, fmt.Errorf("parsing entry: %w", err)
			}
			if err := c.Store.StageObject(ctx, stagingToken, graveler.ValueRecord{
				Key:   graveler.Key(entryRecord.Path),
				Value: entry,
			}); err != nil {
				return nil, nil, fmt.Errorf("staging skipped keys: %w", err)
			}
		}
	}
	mark := it.Marker()
	mark.StagingToken = stagingToken

	return rangeInfo, &mark, nil
}

func (c *Catalog) WriteMetaRange(ctx context.Context, repositoryID string, ranges []*graveler.RangeInfo) (*graveler.MetaRangeInfo, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return c.Store.WriteMetaRange(ctx, repository, ranges)
}

func (c *Catalog) UpdateBranchToken(ctx context.Context, repositoryID, branchID, stagingToken string) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.UpdateBranchToken(ctx, repository, branchID, stagingToken)
}

func (c *Catalog) GetGarbageCollectionRules(ctx context.Context, repositoryID string) (*graveler.GarbageCollectionRules, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return c.Store.GetGarbageCollectionRules(ctx, repository)
}

func (c *Catalog) SetGarbageCollectionRules(ctx context.Context, repositoryID string, rules *graveler.GarbageCollectionRules) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.SetGarbageCollectionRules(ctx, repository, rules)
}

func (c *Catalog) GetBranchProtectionRules(ctx context.Context, repositoryID string) (*graveler.BranchProtectionRules, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}

	return c.Store.GetBranchProtectionRules(ctx, repository)
}

func (c *Catalog) DeleteBranchProtectionRule(ctx context.Context, repositoryID string, pattern string) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.DeleteBranchProtectionRule(ctx, repository, pattern)
}

func (c *Catalog) CreateBranchProtectionRule(ctx context.Context, repositoryID string, pattern string, blockedActions []graveler.BranchProtectionBlockedAction) error {
	repository, err := c.getRepository(ctx, repositoryID)
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
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return c.Store.SaveGarbageCollectionCommits(ctx, repository, previousRunID)
}

// GCUncommittedMark Marks the *next* item to be scanned by the paginated call to PrepareGCUncommitted
type GCUncommittedMark struct {
	BranchID graveler.BranchID `json:"branch"`
	Path     Path              `json:"path"`
	RunID    string            `json:"run_id"`
	Key      string            `json:"key"`
}

type PrepareGCUncommittedInfo struct {
	RunID    string `json:"run_id"`
	Location string `json:"location"`
	Filename string `json:"filename"`
	Mark     *GCUncommittedMark
}

type UncommittedParquetObject struct {
	PhysicalAddress string `parquet:"name=physical_address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CreationDate    int64  `parquet:"name=creation_date, type=INT64, convertedtype=INT_64"`
}

func (c *Catalog) uploadFile(ctx context.Context, ns graveler.StorageNamespace, location string, fd *os.File, size int64) (string, error) {
	_, err := fd.Seek(0, 0)
	if err != nil {
		return "", err
	}
	// location is full path to underlying storage - join a unique filename and upload data
	name := xid.New().String()
	identifier, err := url.JoinPath(location, name)
	if err != nil {
		return "", err
	}
	obj := block.ObjectPointer{
		StorageNamespace: ns.String(),
		Identifier:       identifier,
		IdentifierType:   block.IdentifierTypeFull,
	}
	err = c.BlockAdapter.Put(ctx, obj, size, fd, block.PutOpts{})
	if err != nil {
		return "", err
	}
	return name, nil
}

func (c *Catalog) PrepareGCUncommitted(ctx context.Context, repositoryID string, mark *GCUncommittedMark) (*PrepareGCUncommittedInfo, error) {
	var err error
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}

	var runID string
	if mark == nil {
		runID = c.Store.GCNewRunID()
	} else {
		runID = mark.RunID
	}

	fd, err := os.CreateTemp("", "")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = fd.Close()
		if err := os.Remove(fd.Name()); err != nil {
			c.log.WithField("filename", fd.Name()).Warn("Failed to delete temporary gc uncommitted data file")
		}
	}()

	uw := NewUncommittedWriter(fd)

	// Write parquet to local storage
	newMark, hasData, err := gcWriteUncommitted(ctx, c.Store, repository, uw, mark, runID, int64(c.GCMaxUncommittedFileSize))
	if err != nil {
		return nil, err
	}

	// Upload parquet file to object store
	var (
		uncommittedLocation string
		name                string
	)
	if hasData {
		uncommittedLocation, err = c.Store.GCGetUncommittedLocation(repository, runID)
		if err != nil {
			return nil, err
		}

		name, err = c.uploadFile(ctx, repository.StorageNamespace, uncommittedLocation, fd, uw.Size())
		if err != nil {
			return nil, err
		}
	}

	return &PrepareGCUncommittedInfo{
		Mark:     newMark,
		RunID:    runID,
		Location: uncommittedLocation,
		Filename: name,
	}, nil
}

// CopyEntry copy entry information by using the block adapter to make a copy of the data to a new physical address.
func (c *Catalog) CopyEntry(ctx context.Context, srcRepository, srcRef, srcPath, destRepository, destBranch, destPath string) (*DBEntry, error) {
	// copyObjectFull copy data from srcEntry's physical address (if set) or srcPath into destPath
	// fetch src entry if needed - optimization in case we already have the entry
	srcEntry, err := c.GetEntry(ctx, srcRepository, srcRef, srcPath, GetEntryParams{})
	if err != nil {
		return nil, err
	}

	// load repositories information for storage namespace
	destRepo, err := c.GetRepository(ctx, destRepository)
	if err != nil {
		return nil, err
	}

	srcRepo := destRepo
	if srcRepository != destRepository {
		srcRepo, err = c.GetRepository(ctx, srcRepository)
		if err != nil {
			return nil, err
		}
	}

	// copy data to a new physical address
	dstEntry := *srcEntry
	dstEntry.CreationDate = time.Now()
	dstEntry.Path = destPath
	dstEntry.AddressType = AddressTypeRelative
	dstEntry.PhysicalAddress = c.PathProvider.NewPath()
	srcObject := block.ObjectPointer{
		StorageNamespace: srcRepo.StorageNamespace,
		IdentifierType:   srcEntry.AddressType.ToIdentifierType(),
		Identifier:       srcEntry.PhysicalAddress,
	}
	destObj := block.ObjectPointer{
		StorageNamespace: destRepo.StorageNamespace,
		IdentifierType:   dstEntry.AddressType.ToIdentifierType(),
		Identifier:       dstEntry.PhysicalAddress,
	}
	err = c.BlockAdapter.Copy(ctx, srcObject, destObj)
	if err != nil {
		return nil, err
	}

	// create entry for the final copy
	err = c.CreateEntry(ctx, destRepository, destBranch, dstEntry)
	if err != nil {
		return nil, err
	}
	return &dstEntry, nil
}

func (c *Catalog) SetLinkAddress(ctx context.Context, repository, token string) error {
	repo, err := c.getRepository(ctx, repository)
	if err != nil {
		return err
	}
	return c.Store.SetLinkAddress(ctx, repo, token)
}

func (c *Catalog) VerifyLinkAddress(ctx context.Context, repository, token string) error {
	repo, err := c.getRepository(ctx, repository)
	if err != nil {
		return err
	}
	return c.Store.VerifyLinkAddress(ctx, repo, token)
}

func (c *Catalog) DeleteExpiredLinkAddresses(ctx context.Context) {
	repos, err := c.listRepositoriesHelper(ctx)
	if err != nil {
		c.log.WithError(err).Warn("Failed list repositories during delete expired addresses")
		return
	}

	for _, repo := range repos {
		err := c.Store.DeleteExpiredLinkAddresses(ctx, repo)
		if err != nil {
			c.log.WithError(err).WithField("repository", repo.RepositoryID).Warn("Delete expired address tokens failed")
		}
	}
}

func (c *Catalog) listRepositoriesHelper(ctx context.Context) ([]*graveler.RepositoryRecord, error) {
	it, err := c.Store.ListRepositories(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var repos []*graveler.RepositoryRecord
	for it.Next() {
		repos = append(repos, it.Value())
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return repos, nil
}

func (c *Catalog) Close() error {
	var errs error
	for _, manager := range c.managers {
		err := manager.Close()
		if err != nil {
			_ = multierror.Append(errs, err)
		}
	}
	c.workPool.StopAndWaitFor(workersMaxDrainDuration)
	return errs
}

// dereferenceCommitID dereference 'ref' to a commit ID, this helper makes sure we do not point to explicit branch staging
func (c *Catalog) dereferenceCommitID(ctx context.Context, repository *graveler.RepositoryRecord, ref graveler.Ref) (graveler.CommitID, error) {
	resolvedRef, err := c.Store.Dereference(ctx, repository, ref)
	if err != nil {
		return "", err
	}
	if resolvedRef.CommitID == "" {
		return "", fmt.Errorf("%w: no commit", graveler.ErrInvalidRef)
	}
	if resolvedRef.ResolvedBranchModifier == graveler.ResolvedBranchModifierStaging {
		return "", fmt.Errorf("%w: should point to a commit", graveler.ErrInvalidRef)
	}
	return resolvedRef.CommitID, nil
}

func (c *Catalog) checkCommitIDDuplication(ctx context.Context, repository *graveler.RepositoryRecord, id graveler.CommitID) error {
	_, err := c.Store.GetCommit(ctx, repository, id)
	if err == nil {
		return fmt.Errorf("commit ID %s: %w", id, graveler.ErrConflictFound)
	}
	if errors.Is(err, graveler.ErrNotFound) {
		return nil
	}

	return err
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

func NewUncommittedWriter(writer io.Writer) *UncommittedWriter {
	return &UncommittedWriter{
		writer: writer,
	}
}

// UncommittedWriter wraps io.Writer and tracks the total size of writes done on this writer
// Used to get the current file size written without expensive calls to Flush and Stat
type UncommittedWriter struct {
	size   int64
	writer io.Writer
}

func (w *UncommittedWriter) Write(p []byte) (n int, err error) {
	n, err = w.writer.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *UncommittedWriter) Size() int64 {
	return w.size
}
