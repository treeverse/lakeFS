package catalog

import (
	"bytes"
	"container/heap"
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/alitto/pond"
	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-multierror"
	lru "github.com/hnlq715/golang-lru"
	"github.com/rs/xid"
	blockfactory "github.com/treeverse/lakefs/modules/block/factory"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/block"
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
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/pyramid"
	pyramidparams "github.com/treeverse/lakefs/pkg/pyramid/params"
	"github.com/treeverse/lakefs/pkg/upload"
	"github.com/treeverse/lakefs/pkg/validator"
	"go.uber.org/atomic"
	"go.uber.org/ratelimit"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// hashAlg is the hashing algorithm to use to generate graveler identifiers.  Changing it
	// causes all old identifiers to change, so while existing installations will continue to
	// function they will be unable to re-use any existing objects.
	hashAlg = crypto.SHA256

	NumberOfParentsOfNonMergeCommit = 1

	gcParquetParallelNum = 1 // Number of goroutines to handle marshaling of data

	// Calculation of size deviation by gcPeriodicCheckSize value
	// "data" prefix = 4 bytes
	// partition id = 20 bytes (xid)
	// object name = 20 bytes (xid)
	// timestamp (int64) = 8 bytes
	//
	// Total per entry ~52 bytes
	// Deviation with gcPeriodicCheckSize = 100000 will be around 5 MB
	gcPeriodicCheckSize = 100000

	DumpRefsTaskIDPrefix    = "DR"
	RestoreRefsTaskIDPrefix = "RR"

	TaskExpiryTime = 24 * time.Hour

	// LinkAddressTime the time address is valid from get to link
	LinkAddressTime             = 6 * time.Hour
	LinkAddressSigningDelimiter = ","
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
	graveler.Collaborator
}

const (
	RangeFSName     = "range"
	MetaRangeFSName = "meta-range"
)

const (
	DefaultPathDelimiter = "/"
)

type DiffParams struct {
	Limit            int
	After            string
	Prefix           string
	Delimiter        string
	AdditionalFields []string // db fields names that will be load in additional to Path on Difference's Entry
}

type RevertParams struct {
	Reference    string // the commit to revert
	ParentNumber int    // if reverting a merge commit, the change will be reversed relative to this parent number (1-based).
	Committer    string
	AllowEmpty   bool // allow empty commit (revert without changes)
	*graveler.CommitOverrides
}

type CherryPickParams struct {
	Reference    string // the commit to pick
	ParentNumber *int   // if a merge commit was picked, the change will be applied relative to this parent number (1-based).
	Committer    string
	*graveler.CommitOverrides
}

type PathRecord struct {
	Path     Path
	IsPrefix bool
}

type LogParams struct {
	PathList      []PathRecord
	FromReference string
	Amount        int
	Limit         bool
	FirstParent   bool
	Since         *time.Time
	StopAt        string
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
	// StageOnly when true will return entry found on stage without checking committed data
	StageOnly bool
}

type WriteRangeRequest struct {
	SourceURI         string
	Prepend           string
	After             string
	StagingToken      string
	ContinuationToken string
}

type Config struct {
	Config                config.Config
	KVStore               kv.Store
	SettingsManagerOption settings.ManagerOption
	PathProvider          *upload.PathPartitionProvider
}

type Catalog struct {
	BlockAdapter          block.Adapter
	Store                 Store
	managers              []io.Closer
	workPool              *pond.WorkerPool
	PathProvider          *upload.PathPartitionProvider
	BackgroundLimiter     ratelimit.Limiter
	KVStore               kv.Store
	KVStoreLimited        kv.Store
	addressProvider       *ident.HexAddressProvider
	deleteSensor          *graveler.DeleteSensor
	UGCPrepareMaxFileSize int64
	UGCPrepareInterval    time.Duration
	signingKey            config.SecureString
}

const (
	ListRepositoriesLimitMax = 1000
	ListBranchesLimitMax     = 1000
	ListTagsLimitMax         = 1000
	DiffLimitMax             = 1000
	ListPullsLimitMax        = 1000
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
	Force  bool
}

type ctxCloser struct {
	close context.CancelFunc
}

func (c *ctxCloser) Close() error {
	go c.close()
	return nil
}

func makeBranchApproximateOwnershipParams(cfg config.ApproximatelyCorrectOwnership) ref.BranchApproximateOwnershipParams {
	if !cfg.Enabled {
		// zero Durations => no branch ownership
		return ref.BranchApproximateOwnershipParams{}
	}
	return ref.BranchApproximateOwnershipParams{
		AcquireInterval: cfg.Acquire,
		RefreshInterval: cfg.Refresh,
	}
}

func New(ctx context.Context, cfg Config) (*Catalog, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	adapter, err := blockfactory.BuildBlockAdapter(ctx, nil, cfg.Config)
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("build block adapter: %w", err)
	}

	baseCfg := cfg.Config.GetBaseConfig()
	tierFSParams, err := pyramidparams.NewCommittedTierFSParams(baseCfg, adapter)
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("configure tiered FS for committed: %w", err)
	}
	metaRangeFS, err := pyramid.NewFS(&pyramidparams.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              MetaRangeFSName,
		DiskAllocProportion: tierFSParams.MetaRangeAllocationProportion,
	})
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("create tiered FS for committed metaranges: %w", err)
	}

	rangeFS, err := pyramid.NewFS(&pyramidparams.InstanceParams{
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

	committedManager, closers, err := buildCommittedManager(cfg, pebbleSSTableCache, rangeFS, metaRangeFS)
	if err != nil {
		cancelFn()
		return nil, err
	}

	executor := batch.NewConditionalExecutor(logging.ContextUnavailable())
	go executor.Run(ctx)

	// Setup rate limiter used for background operations
	limiter := newLimiter(baseCfg.Graveler.Background.RateLimit)

	storeLimiter := kv.NewStoreLimiter(cfg.KVStore, limiter)
	addressProvider := ident.NewHexAddressProvider()

	refManager := ref.NewRefManager(
		ref.ManagerConfig{
			Executor:                         executor,
			KVStore:                          cfg.KVStore,
			KVStoreLimited:                   storeLimiter,
			AddressProvider:                  addressProvider,
			RepositoryCacheConfig:            ref.CacheConfig(baseCfg.Graveler.RepositoryCache),
			CommitCacheConfig:                ref.CacheConfig(baseCfg.Graveler.CommitCache),
			MaxBatchDelay:                    baseCfg.Graveler.MaxBatchDelay,
			BranchApproximateOwnershipParams: makeBranchApproximateOwnershipParams(baseCfg.Graveler.BranchOwnership),
		},
		cfg.Config.StorageConfig(),
	)
	gcManager := retention.NewGarbageCollectionManager(tierFSParams.Adapter, refManager, baseCfg.Committed.BlockStoragePrefix)
	settingManager := settings.NewManager(refManager, cfg.KVStore)
	if cfg.SettingsManagerOption != nil {
		cfg.SettingsManagerOption(settingManager)
	}

	protectedBranchesManager := branch.NewProtectionManager(settingManager)
	stagingManager := staging.NewManager(ctx, cfg.KVStore, storeLimiter, baseCfg.Graveler.BatchDBIOTransactionMarkers, executor)
	var deleteSensor *graveler.DeleteSensor
	if baseCfg.Graveler.CompactionSensorThreshold > 0 {
		cb := func(repositoryID graveler.RepositoryID, branchID graveler.BranchID, stagingTokenID graveler.StagingToken, inGrace bool) {
			logging.FromContext(ctx).WithFields(logging.Fields{
				"repositoryID":   repositoryID,
				"branchID":       branchID,
				"stagingTokenID": stagingTokenID,
				"inGrace":        inGrace,
			}).Info("Delete sensor callback")
		}
		deleteSensor = graveler.NewDeleteSensor(baseCfg.Graveler.CompactionSensorThreshold, cb)
	}
	gStore := graveler.NewGraveler(committedManager, stagingManager, refManager, gcManager, protectedBranchesManager, deleteSensor)

	// The size of the workPool is determined by the number of workers and the number of desired pending tasks for each worker.
	workPool := pond.New(sharedWorkers, sharedWorkers*pendingTasksPerWorker, pond.Context(ctx))
	closers = append(closers, &ctxCloser{cancelFn})
	return &Catalog{
		BlockAdapter:          tierFSParams.Adapter,
		Store:                 gStore,
		UGCPrepareMaxFileSize: baseCfg.UGC.PrepareMaxFileSize,
		UGCPrepareInterval:    baseCfg.UGC.PrepareInterval,
		PathProvider:          cfg.PathProvider,
		BackgroundLimiter:     limiter,
		workPool:              workPool,
		KVStore:               cfg.KVStore,
		managers:              closers,
		KVStoreLimited:        storeLimiter,
		addressProvider:       addressProvider,
		deleteSensor:          deleteSensor,
		signingKey:            cfg.Config.StorageConfig().SigningKey(),
	}, nil
}

func buildCommittedManager(cfg Config, pebbleSSTableCache *pebble.Cache, rangeFS pyramid.FS, metaRangeFS pyramid.FS) (graveler.CommittedManager, []io.Closer, error) {
	baseCfg := cfg.Config.GetBaseConfig()
	committedParams := committed.Params{
		MinRangeSizeBytes:          baseCfg.Committed.Permanent.MinRangeSizeBytes,
		MaxRangeSizeBytes:          baseCfg.Committed.Permanent.MaxRangeSizeBytes,
		RangeSizeEntriesRaggedness: baseCfg.Committed.Permanent.RangeRaggednessEntries,
		MaxUploaders:               baseCfg.Committed.LocalCache.MaxUploadersPerWriter,
	}
	var closers []io.Closer
	sstableManagers := make(map[graveler.StorageID]committed.RangeManager)
	sstableMetaRangeManagers := make(map[graveler.StorageID]committed.MetaRangeManager)
	storageIDs := cfg.Config.StorageConfig().GetStorageIDs()
	for _, sID := range storageIDs {
		sstableRangeManager := sstable.NewPebbleSSTableRangeManager(pebbleSSTableCache, rangeFS, hashAlg, committed.StorageID(sID))
		sstableManagers[graveler.StorageID(sID)] = sstableRangeManager
		closers = append(closers, sstableRangeManager)

		storage := cfg.Config.StorageConfig().GetStorageByID(sID)
		if storage.IsBackwardsCompatible() {
			sstableManagers[config.SingleBlockstoreID] = sstableRangeManager
		}

		sstableMetaManager := sstable.NewPebbleSSTableRangeManager(pebbleSSTableCache, metaRangeFS, hashAlg, committed.StorageID(sID))
		closers = append(closers, sstableMetaManager)

		sstableMetaRangeManager, err := committed.NewMetaRangeManager(
			committedParams,
			sstableMetaManager,
			sstableRangeManager,
			graveler.StorageID(sID),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("create SSTable-based metarange manager: %w", err)
		}
		sstableMetaRangeManagers[graveler.StorageID(sID)] = sstableMetaRangeManager
		if storage.IsBackwardsCompatible() {
			sstableMetaRangeManagers[config.SingleBlockstoreID] = sstableMetaRangeManager
		}
	}
	committedManager := committed.NewCommittedManager(sstableMetaRangeManagers, sstableManagers, committedParams)
	return committedManager, closers, nil
}

func newLimiter(rateLimit int) ratelimit.Limiter {
	var limiter ratelimit.Limiter
	if rateLimit == 0 {
		limiter = ratelimit.NewUnlimited()
	} else {
		limiter = ratelimit.New(rateLimit)
	}
	return limiter
}

func (c *Catalog) SetHooksHandler(hooks graveler.HooksHandler) {
	c.Store.SetHooksHandler(hooks)
}

func (c *Catalog) log(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx).WithField("service_name", "entry_catalog")
}

// CreateRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with default branch name 'branch'
func (c *Catalog) CreateRepository(ctx context.Context, repository string, storageID string, storageNamespace string, branch string, readOnly bool) (*Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	storageIdentifier := graveler.StorageID(storageID)
	storageNS := graveler.StorageNamespace(storageNamespace)
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "name", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "storageNamespace", Value: storageNS, Fn: graveler.ValidateStorageNamespace},
	}); err != nil {
		return nil, err
	}

	repo, err := c.Store.CreateRepository(ctx, repositoryID, storageIdentifier, storageNS, branchID, readOnly)
	if err != nil {
		return nil, err
	}
	catalogRepo := &Repository{
		Name:             repositoryID.String(),
		StorageID:        storageIdentifier.String(),
		StorageNamespace: storageNS.String(),
		DefaultBranch:    branchID.String(),
		CreationDate:     repo.CreationDate,
		ReadOnly:         repo.ReadOnly,
	}
	return catalogRepo, nil
}

// CreateBareRepository create a new repository pointing to 'storageNamespace' (ex: s3://bucket1/repo) with no initial branch or commit
// defaultBranchID will point to a non-existent branch on creation, it is up to the caller to eventually create it.
func (c *Catalog) CreateBareRepository(ctx context.Context, repository string, storageID string, storageNamespace string, defaultBranchID string, readOnly bool) (*Repository, error) {
	repositoryID := graveler.RepositoryID(repository)
	storageIdentifier := graveler.StorageID(storageID)
	storageNS := graveler.StorageNamespace(storageNamespace)
	branchID := graveler.BranchID(defaultBranchID)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "name", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "storageNamespace", Value: storageNS, Fn: graveler.ValidateStorageNamespace},
	}); err != nil {
		return nil, err
	}

	repo, err := c.Store.CreateBareRepository(ctx, repositoryID, storageIdentifier, storageNS, branchID, readOnly)
	if err != nil {
		return nil, err
	}
	catalogRepo := &Repository{
		Name:             repositoryID.String(),
		StorageID:        storageIdentifier.String(),
		StorageNamespace: storageNS.String(),
		DefaultBranch:    branchID.String(),
		CreationDate:     repo.CreationDate,
		ReadOnly:         readOnly,
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
		StorageID:        repo.StorageID.String(),
		StorageNamespace: repo.StorageNamespace.String(),
		DefaultBranch:    repo.DefaultBranchID.String(),
		CreationDate:     repo.CreationDate,
		ReadOnly:         repo.ReadOnly,
	}
	return catalogRepository, nil
}

// DeleteRepository delete a repository
func (c *Catalog) DeleteRepository(ctx context.Context, repository string, opts ...graveler.SetOptionsFunc) error {
	repositoryID := graveler.RepositoryID(repository)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return err
	}
	return c.Store.DeleteRepository(ctx, repositoryID, opts...)
}

// GetRepositoryMetadata get repository metadata
func (c *Catalog) GetRepositoryMetadata(ctx context.Context, repository string) (graveler.RepositoryMetadata, error) {
	repositoryID := graveler.RepositoryID(repository)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	return c.Store.GetRepositoryMetadata(ctx, repositoryID)
}

// UpdateRepositoryMetadata set repository metadata
func (c *Catalog) UpdateRepositoryMetadata(ctx context.Context, repository string, metadata graveler.RepositoryMetadata) error {
	if len(metadata) == 0 {
		return nil
	}
	r, err := c.getRepository(ctx, repository)
	if err != nil {
		return err
	}
	return c.Store.SetRepositoryMetadata(ctx, r, func(md graveler.RepositoryMetadata) (graveler.RepositoryMetadata, error) {
		if md == nil {
			return metadata, nil
		}
		for k, v := range metadata {
			md[k] = v
		}
		return md, nil
	})
}

// DeleteRepositoryMetadata delete repository metadata
func (c *Catalog) DeleteRepositoryMetadata(ctx context.Context, repository string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	r, err := c.getRepository(ctx, repository)
	if err != nil {
		return err
	}
	return c.Store.SetRepositoryMetadata(ctx, r, func(md graveler.RepositoryMetadata) (graveler.RepositoryMetadata, error) {
		for _, k := range keys {
			delete(md, k)
		}
		return md, nil
	})
}

// ListRepositories list repository information, the bool returned is true when more repositories can be listed.
// In this case, pass the last repository name as 'after' on the next call to ListRepositories. Results can be
// filtered by specifying a prefix or, more generally, a searchString.
func (c *Catalog) ListRepositories(ctx context.Context, limit int, prefix, searchString, after string) ([]*Repository, bool, error) {
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
		if !strings.Contains(string(record.RepositoryID), searchString) {
			continue
		}
		if record.RepositoryID == afterRepositoryID {
			continue
		}
		repos = append(repos, &Repository{
			Name:             record.RepositoryID.String(),
			StorageID:        record.StorageID.String(),
			StorageNamespace: record.StorageNamespace.String(),
			DefaultBranch:    record.DefaultBranchID.String(),
			CreationDate:     record.CreationDate,
			ReadOnly:         record.ReadOnly,
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

func (c *Catalog) CreateBranch(ctx context.Context, repositoryID string, branch string, sourceBranch string, opts ...graveler.SetOptionsFunc) (*CommitLog, error) {
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

	// look for a tag with the same name to avoid reference conflict
	if _, err := c.Store.GetTag(ctx, repository, graveler.TagID(branchID)); err == nil {
		return nil, fmt.Errorf("tag ID %s: %w", branchID, graveler.ErrConflictFound)
	} else if !errors.Is(err, graveler.ErrNotFound) {
		return nil, err
	}
	newBranch, err := c.Store.CreateBranch(ctx, repository, branchID, sourceRef, opts...)
	if err != nil {
		return nil, err
	}
	commit, err := c.Store.GetCommit(ctx, repository, newBranch.CommitID)
	if err != nil {
		return nil, err
	}
	catalogCommitLog := &CommitLog{
		Reference:  newBranch.CommitID.String(),
		Committer:  commit.Committer,
		Message:    commit.Message,
		Metadata:   Metadata(commit.Metadata),
		Version:    CommitVersion(commit.Version),
		Generation: CommitGeneration(commit.Generation),
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, string(parent))
	}
	return catalogCommitLog, nil
}

func (c *Catalog) DeleteBranch(ctx context.Context, repositoryID string, branch string, opts ...graveler.SetOptionsFunc) error {
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
	return c.Store.DeleteBranch(ctx, repository, branchID, opts...)
}

func (c *Catalog) ListBranches(ctx context.Context, repositoryID string, prefix string, limit int, after string, opts ...graveler.ListOptionsFunc) ([]*Branch, bool, error) {
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
	it, err := c.Store.ListBranches(ctx, repository, opts...)
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
	// return results (optionally trimmed) and hasMore
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

func (c *Catalog) HardResetBranch(ctx context.Context, repositoryID, branch, refExpr string, opts ...graveler.SetOptionsFunc) error {
	branchID := graveler.BranchID(branch)
	reference := graveler.Ref(refExpr)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
		{Name: "ref", Value: reference, Fn: graveler.ValidateRef},
	}); err != nil {
		return err
	}
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.ResetHard(ctx, repository, branchID, reference, opts...)
}

func (c *Catalog) ResetBranch(ctx context.Context, repositoryID string, branch string, opts ...graveler.SetOptionsFunc) error {
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
	return c.Store.Reset(ctx, repository, branchID, opts...)
}

func (c *Catalog) CreateTag(ctx context.Context, repositoryID string, tagID string, ref string, opts ...graveler.SetOptionsFunc) (string, error) {
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

	// look for a branch with the same name to avoid reference conflict
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
	err = c.Store.CreateTag(ctx, repository, tag, commitID, opts...)
	if err != nil {
		return "", err
	}
	return commitID.String(), nil
}

func (c *Catalog) DeleteTag(ctx context.Context, repositoryID string, tagID string, opts ...graveler.SetOptionsFunc) error {
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
	return c.Store.DeleteTag(ctx, repository, tag, opts...)
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
	// return results (optionally trimmed) and hasMore
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

// GetEntry returns the current entry for a path in repository branch reference.  Returns
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

// UpdateEntryUserMetadata updates user metadata for the current entry for a
// path in repository branch reference.
func (c *Catalog) UpdateEntryUserMetadata(ctx context.Context, repositoryID, branch, path string, newUserMetadata map[string]string) error {
	branchID := graveler.BranchID(branch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "branch", Value: branchID, Fn: graveler.ValidateBranchID},
		{Name: "path", Value: Path(path), Fn: ValidatePath},
	}); err != nil {
		return err
	}

	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil
	}

	key := graveler.Key(path)
	updater := graveler.ValueUpdateFunc(func(value *graveler.Value) (*graveler.Value, error) {
		if value == nil {
			return nil, fmt.Errorf("update user metadata on %s/%s/%s: %w",
				repositoryID, branchID, path, graveler.ErrNotFound)
		}
		entry, err := ValueToEntry(value)
		if err != nil {
			return nil, err
		}
		entry.Metadata = newUserMetadata
		return EntryToValue(entry)
	})
	return c.Store.Update(ctx, repository, branchID, key, updater)
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

func (c *Catalog) DeleteEntry(ctx context.Context, repositoryID string, branch string, path string, opts ...graveler.SetOptionsFunc) error {
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
	return c.Store.Delete(ctx, repository, branchID, key, opts...)
}

func (c *Catalog) DeleteEntries(ctx context.Context, repositoryID string, branch string, paths []string, opts ...graveler.SetOptionsFunc) error {
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
	return c.Store.DeleteBatch(ctx, repository, branchID, keys, opts...)
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

func (c *Catalog) ResetEntry(ctx context.Context, repositoryID string, branch string, path string, opts ...graveler.SetOptionsFunc) error {
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
	return c.Store.ResetKey(ctx, repository, branchID, key, opts...)
}

func (c *Catalog) ResetEntries(ctx context.Context, repositoryID string, branch string, prefix string, opts ...graveler.SetOptionsFunc) error {
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
	return c.Store.ResetPrefix(ctx, repository, branchID, keyPrefix, opts...)
}

func (c *Catalog) Commit(ctx context.Context, repositoryID, branch, message, committer string, metadata Metadata, date *int64, sourceMetarange *string, allowEmpty bool, opts ...graveler.SetOptionsFunc) (*CommitLog, error) {
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
		Committer:  committer,
		Message:    message,
		Date:       date,
		Metadata:   map[string]string(metadata),
		AllowEmpty: allowEmpty,
	}
	if sourceMetarange != nil {
		x := graveler.MetaRangeID(*sourceMetarange)
		p.SourceMetaRange = &x
	}
	commitID, err := c.Store.Commit(ctx, repository, branchID, p, opts...)
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
	catalogCommitLog.MetaRangeID = string(commit.MetaRangeID)
	catalogCommitLog.Version = CommitVersion(commit.Version)
	catalogCommitLog.Generation = CommitGeneration(commit.Generation)
	return catalogCommitLog, nil
}

func (c *Catalog) CreateCommitRecord(ctx context.Context, repositoryID string, commitID string, version int, committer string, message string, metaRangeID string, creationDate int64, parents []string, metadata map[string]string, generation int32, opts ...graveler.SetOptionsFunc) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	commitParents := make([]graveler.CommitID, len(parents))
	for i, parent := range parents {
		commitParents[i] = graveler.CommitID(parent)
	}
	commit := graveler.Commit{
		// cast from int32 to int. no information loss danger
		Version:      graveler.CommitVersion(version), //nolint:gosec
		Committer:    committer,
		Message:      message,
		MetaRangeID:  graveler.MetaRangeID(metaRangeID),
		CreationDate: time.Unix(creationDate, 0).UTC(),
		Parents:      commitParents,
		Metadata:     metadata,
		// cast from int32 to int32
		Generation: graveler.CommitGeneration(generation),
	}
	return c.Store.CreateCommitRecord(ctx, repository, graveler.CommitID(commitID), commit, opts...)
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
		Generation:   CommitGeneration(commit.Generation),
		Version:      CommitVersion(commit.Version),
		Parents:      []string{},
	}
	for _, parent := range commit.Parents {
		catalogCommitLog.Parents = append(catalogCommitLog.Parents, string(parent))
	}
	return catalogCommitLog, nil
}

func (c *Catalog) ListCommits(ctx context.Context, repositoryID string, ref string, params LogParams) ([]*CommitLog, bool, error) {
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "ref", Value: graveler.Ref(ref), Fn: graveler.ValidateRef},
	}); err != nil {
		return nil, false, err
	}

	// disabling batching for this flow. See #3935 for more details
	ctx = context.WithValue(ctx, batch.SkipBatchContextKey, struct{}{})
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, false, err
	}

	commitID, err := c.dereferenceCommitID(ctx, repository, graveler.Ref(ref))
	if err != nil {
		return nil, false, fmt.Errorf("ref: %w", err)
	}
	if params.StopAt != "" {
		stopAtCommitID, err := c.dereferenceCommitID(ctx, repository, graveler.Ref(params.StopAt))
		if err != nil {
			return nil, false, fmt.Errorf("stop_at: %w", err)
		}
		params.StopAt = stopAtCommitID.String()
	}
	it, err := c.Store.Log(ctx, repository, commitID, params.FirstParent, params.Since)
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

	// iterate over commits log and push work into the work channel
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
		// wait until workers are done or the first non-nil error was returned from a worker
		if err := workerGroup.Wait(); err != nil {
			// Wait until all workers are done regardless of the error.
			workerGroup.TaskGroup.Wait()
			return err
		}
		return it.Err()
	})

	// process out the channel to keep order into results channel by using heap
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
		len(commits) >= params.Amount+1 || (len(commits) > 0 && commits[len(commits)-1].Reference == params.StopAt)
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
		Version:      CommitVersion(val.Version),
		Generation:   CommitGeneration(val.Generation),
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

func (c *Catalog) Revert(ctx context.Context, repositoryID string, branch string, params RevertParams, opts ...graveler.SetOptionsFunc) error {
	branchID := graveler.BranchID(branch)
	reference := graveler.Ref(params.Reference)
	commitParams := graveler.CommitParams{
		Committer:  params.Committer,
		Message:    fmt.Sprintf("Revert %s", params.Reference),
		AllowEmpty: params.AllowEmpty,
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
	_, err = c.Store.Revert(ctx, repository, branchID, reference, parentNumber, commitParams, params.CommitOverrides, opts...)
	return err
}

func (c *Catalog) CherryPick(ctx context.Context, repositoryID string, branch string, params CherryPickParams, opts ...graveler.SetOptionsFunc) (*CommitLog, error) {
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

	commitID, err := c.Store.CherryPick(ctx, repository, branchID, reference, parentNumber, params.Committer, params.CommitOverrides, opts...)
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
		Version:      CommitVersion(commit.Version),
		Generation:   CommitGeneration(commit.Generation),
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

func (c *Catalog) Merge(ctx context.Context, repositoryID string, destinationBranch string, sourceRef string, committer string, message string, metadata Metadata, strategy string, opts ...graveler.SetOptionsFunc) (string, error) {
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

	commitID, err := c.Store.Merge(ctx, repository, destination, source, commitParams, strategy, opts...)
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

func (c *Catalog) DumpRepositorySubmit(ctx context.Context, repositoryID string) (string, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}

	taskStatus := &RepositoryDumpStatus{}
	taskSteps := []taskStep{
		{
			Name: "dump commits",
			Func: func(ctx context.Context) error {
				commitsMetaRangeID, err := c.Store.DumpCommits(ctx, repository)
				if err != nil {
					return err
				}
				taskStatus.Info = &RepositoryDumpInfo{
					CommitsMetarangeId: string(*commitsMetaRangeID),
				}
				return nil
			},
		},
		{
			Name: "dump branches",
			Func: func(ctx context.Context) error {
				branchesMetaRangeID, err := c.Store.DumpBranches(ctx, repository)
				if err != nil {
					return err
				}
				taskStatus.Info.BranchesMetarangeId = string(*branchesMetaRangeID)
				return nil
			},
		},
		{
			Name: "dump tags",
			Func: func(ctx context.Context) error {
				tagsMetaRangeID, err := c.Store.DumpTags(ctx, repository)
				if err != nil {
					return err
				}
				taskStatus.Info.TagsMetarangeId = string(*tagsMetaRangeID)
				return nil
			},
		},
	}

	// create refs dump task and update initial status.
	taskID := NewTaskID(DumpRefsTaskIDPrefix)
	err = c.runBackgroundTaskSteps(repository, taskID, taskSteps, taskStatus)
	if err != nil {
		return "", err
	}
	return taskID, nil
}

func (c *Catalog) DumpRepositoryStatus(ctx context.Context, repositoryID string, id string) (*RepositoryDumpStatus, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	if !IsTaskID(DumpRefsTaskIDPrefix, id) {
		return nil, graveler.ErrNotFound
	}

	var taskStatus RepositoryDumpStatus
	err = GetTaskStatus(ctx, c.KVStore, repository, id, &taskStatus)
	if err != nil {
		return nil, err
	}
	return &taskStatus, nil
}

func (c *Catalog) RestoreRepositorySubmit(ctx context.Context, repositoryID string, info *RepositoryDumpInfo, opts ...graveler.SetOptionsFunc) (string, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}

	// verify bare repository - no commits
	_, _, err = c.ListCommits(ctx, repository.RepositoryID.String(), repository.DefaultBranchID.String(), LogParams{
		Amount: 1,
		Limit:  true,
	})
	if !errors.Is(err, graveler.ErrNotFound) {
		return "", ErrNonEmptyRepository
	}

	// create refs restore task and update initial status
	taskStatus := &RepositoryRestoreStatus{}
	taskSteps := []taskStep{
		{
			Name: "load commits",
			Func: func(ctx context.Context) error {
				return c.Store.LoadCommits(ctx, repository, graveler.MetaRangeID(info.CommitsMetarangeId), opts...)
			},
		},
		{
			Name: "load branches",
			Func: func(ctx context.Context) error {
				return c.Store.LoadBranches(ctx, repository, graveler.MetaRangeID(info.BranchesMetarangeId), opts...)
			},
		},
		{
			Name: "load tags",
			Func: func(ctx context.Context) error {
				return c.Store.LoadTags(ctx, repository, graveler.MetaRangeID(info.TagsMetarangeId), opts...)
			},
		},
	}
	taskID := NewTaskID(RestoreRefsTaskIDPrefix)
	if err := c.runBackgroundTaskSteps(repository, taskID, taskSteps, taskStatus); err != nil {
		return "", err
	}
	return taskID, nil
}

func (c *Catalog) RestoreRepositoryStatus(ctx context.Context, repositoryID string, id string) (*RepositoryRestoreStatus, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	if !IsTaskID(RestoreRefsTaskIDPrefix, id) {
		return nil, graveler.ErrNotFound
	}

	var status RepositoryRestoreStatus
	err = GetTaskStatus(ctx, c.KVStore, repository, id, &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

// runBackgroundTaskSteps update task status provided after filling the 'Task' field and update for each step provided.
// the task status is updated after each step, and the task is marked as completed if the step is the last one.
// initial update if the task is done before running the steps.
func (c *Catalog) runBackgroundTaskSteps(repository *graveler.RepositoryRecord, taskID string, steps []taskStep, taskStatus protoreflect.ProtoMessage) error {
	// Allocate Task and set if on the taskStatus's 'Task' field.
	// We continue to update this field while running each step.
	// If the task field in the common Protobuf message is changed, we need to update the field name here as well.
	task := &Task{
		Id:        taskID,
		UpdatedAt: timestamppb.Now(),
	}
	reflect.ValueOf(taskStatus).Elem().FieldByName("Task").Set(reflect.ValueOf(task))

	// make sure we use background context as soon as we submit the task the request is done
	ctx := context.Background()

	// initial task update done before we run each step in the background task
	if err := UpdateTaskStatus(ctx, c.KVStore, repository, taskID, taskStatus); err != nil {
		return err
	}

	log := c.log(ctx).WithFields(logging.Fields{"task_id": taskID, "repository": repository.RepositoryID})
	c.workPool.Submit(func() {
		for stepIdx, step := range steps {
			// call the step function
			err := step.Func(ctx)
			// update task part
			task.UpdatedAt = timestamppb.Now()
			if err != nil {
				log.WithError(err).WithField("step", step.Name).Errorf("Catalog background task step failed")
				task.Done = true
				task.Error = err.Error()
			} else if stepIdx == len(steps)-1 {
				task.Done = true
			}

			// update task status
			if err := UpdateTaskStatus(ctx, c.KVStore, repository, taskID, taskStatus); err != nil {
				log.WithError(err).WithField("step", step.Name).Error("Catalog failed to update task status")
			}

			// make sure we stop based on task completed status, as we may fail
			if task.Done {
				break
			}
		}
	})
	return nil
}

// DeleteExpiredRepositoryTasks deletes all expired tasks for the given repository
func (c *Catalog) deleteRepositoryExpiredTasks(ctx context.Context, repo *graveler.RepositoryRecord) error {
	// new scan iterator to iterate over all tasks
	repoPartition := graveler.RepoPartition(repo)
	it, err := kv.NewPrimaryIterator(ctx, c.KVStoreLimited, (&TaskMsg{}).ProtoReflect().Type(),
		repoPartition, []byte(TaskPath("")), kv.IteratorOptionsFrom([]byte("")))
	if err != nil {
		return err
	}
	defer it.Close()

	// iterate over all tasks and delete expired ones
	for it.Next() {
		ent := it.Entry()
		msg := ent.Value.(*TaskMsg)
		if msg.Task == nil {
			continue
		}
		if time.Since(msg.Task.UpdatedAt.AsTime()) < TaskExpiryTime {
			continue
		}
		err := c.KVStoreLimited.Delete(ctx, []byte(repoPartition), ent.Key)
		if err != nil {
			return err
		}
	}
	return it.Err()
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

func (c *Catalog) LoadCommits(ctx context.Context, repositoryID, commitsMetaRangeID string, opts ...graveler.SetOptionsFunc) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.LoadCommits(ctx, repository, graveler.MetaRangeID(commitsMetaRangeID), opts...)
}

func (c *Catalog) LoadBranches(ctx context.Context, repositoryID, branchesMetaRangeID string, opts ...graveler.SetOptionsFunc) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.LoadBranches(ctx, repository, graveler.MetaRangeID(branchesMetaRangeID), opts...)
}

func (c *Catalog) LoadTags(ctx context.Context, repositoryID, tagsMetaRangeID string, opts ...graveler.SetOptionsFunc) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.LoadTags(ctx, repository, graveler.MetaRangeID(tagsMetaRangeID), opts...)
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

func (c *Catalog) importAsync(ctx context.Context, repository *graveler.RepositoryRecord, branchID, importID string, params ImportRequest, logger logging.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	importManager, err := NewImport(ctx, cancel, logger, c.KVStore, repository, importID)
	if err != nil {
		return fmt.Errorf("creating import manager: %w", err)
	}
	defer importManager.Close()

	wg, wgCtx := c.workPool.GroupContext(ctx)
	for _, source := range params.Paths {
		wg.Submit(func() error {
			uri, err := url.Parse(source.Path)
			if err != nil {
				return fmt.Errorf("could not parse storage URI %s: %w", uri, err)
			}

			walker, err := c.BlockAdapter.GetWalker(repository.StorageID.String(), block.WalkerOptions{StorageURI: uri})
			if err != nil {
				return fmt.Errorf("creating object-store walker on path %s: %w", source.Path, err)
			}

			it, err := NewWalkEntryIterator(wgCtx, block.NewWalkerWrapper(walker, uri), source.Type, source.Destination, "", "")
			if err != nil {
				return fmt.Errorf("creating walk iterator on path %s: %w", source.Path, err)
			}

			logger.WithFields(logging.Fields{"source": source.Path, "itr": it}).Debug("Ingest source")
			defer it.Close()
			return importManager.Ingest(it)
		})
	}

	err = wg.Wait()
	if err != nil {
		importError := fmt.Errorf("error on ingest: %w", err)
		importManager.SetError(importError)
		return importError
	}

	importItr, err := importManager.NewItr()
	if err != nil {
		importError := fmt.Errorf("error on import iterator: %w", err)
		importManager.SetError(importError)
		return importError
	}
	defer importItr.Close()

	var ranges []*graveler.RangeInfo
	for importItr.hasMore {
		rangeInfo, err := c.Store.WriteRange(ctx, repository, importItr, graveler.WithForce(params.Force))
		if err != nil {
			importError := fmt.Errorf("write range: %w", err)
			importManager.SetError(importError)
			return importError
		}

		ranges = append(ranges, rangeInfo)
		// Check if operation was canceled
		if ctx.Err() != nil {
			return nil
		}
	}

	// Create metarange
	metarange, err := c.Store.WriteMetaRange(ctx, repository, ranges, graveler.WithForce(params.Force))
	if err != nil {
		importError := fmt.Errorf("create metarange: %w", err)
		importManager.SetError(importError)
		return importError
	}

	prefixes := make([]graveler.Prefix, 0, len(params.Paths))
	for _, ip := range params.Paths {
		prefixes = append(prefixes, graveler.Prefix(ip.Destination))
	}

	if params.Commit.CommitMessage == "" {
		params.Commit.CommitMessage = "Import objects"
	}
	commitID, err := c.Store.Import(ctx, repository, graveler.BranchID(branchID), metarange.ID, graveler.CommitParams{
		Committer: params.Commit.Committer,
		Message:   params.Commit.CommitMessage,
		Metadata:  map[string]string(params.Commit.Metadata),
	}, prefixes, graveler.WithForce(params.Force))
	if err != nil {
		importError := fmt.Errorf("merge import: %w", err)
		importManager.SetError(importError)
		return importError
	}

	commit, err := c.Store.GetCommit(ctx, repository, commitID)
	if err != nil {
		importError := fmt.Errorf("get commit: %w", err)
		importManager.SetError(importError)
		return importError
	}

	// Update import status
	status := importManager.Status()
	status.MetaRangeID = commit.MetaRangeID
	status.Commit = &graveler.CommitRecord{
		CommitID: commitID,
		Commit:   commit,
	}

	status.Completed = true
	importManager.SetStatus(status)
	return nil
}

// verifyImportPaths - Verify that import paths will not cause an import of objects from the repository namespace itself
func verifyImportPaths(storageNamespace string, params ImportRequest) error {
	for _, p := range params.Paths {
		if strings.HasPrefix(p.Path, storageNamespace) {
			return fmt.Errorf("import path (%s) in repository namespace (%s) is prohibited: %w", p.Path, storageNamespace, ErrInvalidImportSource)
		}
		if p.Type == ImportPathTypePrefix && strings.HasPrefix(storageNamespace, p.Path) {
			return fmt.Errorf("prefix (%s) contains repository namespace: (%s), %w", p.Path, storageNamespace, ErrInvalidImportSource)
		}
	}
	return nil
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

	if err = verifyImportPaths(repository.StorageNamespace.String(), params); err != nil {
		return "", err
	}

	id := xid.New().String()
	// Run import
	go func() {
		logger := c.log(ctx).WithField("import_id", id)
		// Passing context.WithoutCancel to avoid canceling the import operation when the wrapping Import function returns,
		// and keep the context's fields intact for next operations (for example, PreCommitHook runs).
		err = c.importAsync(context.WithoutCancel(ctx), repository, branchID, id, params, logger)
		if err != nil {
			logger.WithError(err).Error("import failure")
		}
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
	if importStatus.Completed {
		c.log(ctx).WithFields(logging.Fields{
			"import_id": importID,
			"completed": importStatus.Completed,
			"error":     importStatus.Error,
		}).Warning("Not canceling import - already completed")
		return graveler.ErrConflictFound
	}
	importStatus.Error = ImportCanceled
	importStatus.UpdatedAt = timestamppb.Now()
	return kv.SetMsg(ctx, c.KVStore, graveler.RepoPartition(repository), []byte(graveler.ImportsPath(importID)), importStatus)
}

func (c *Catalog) getImportStatus(ctx context.Context, repository *graveler.RepositoryRecord, importID string) (*graveler.ImportStatusData, error) {
	repoPartition := graveler.RepoPartition(repository)
	data := &graveler.ImportStatusData{}
	_, err := kv.GetMsg(ctx, c.KVStore, repoPartition, []byte(graveler.ImportsPath(importID)), data)
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

func (c *Catalog) WriteRange(ctx context.Context, repositoryID string, params WriteRangeRequest, opts ...graveler.SetOptionsFunc) (*graveler.RangeInfo, *Mark, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, nil, err
	}

	uri, err := url.Parse(params.SourceURI)
	if err != nil {
		return nil, nil, fmt.Errorf("could not parse storage URI %s: %w", uri, err)
	}

	walker, err := c.BlockAdapter.GetWalker(repository.StorageID.String(), block.WalkerOptions{StorageURI: uri})
	if err != nil {
		return nil, nil, fmt.Errorf("creating object-store walker on path %s: %w", params.SourceURI, err)
	}

	it, err := NewWalkEntryIterator(ctx, block.NewWalkerWrapper(walker, uri), ImportPathTypePrefix, params.Prepend, params.After, params.ContinuationToken)
	if err != nil {
		return nil, nil, fmt.Errorf("creating walk iterator: %w", err)
	}
	defer it.Close()

	rangeInfo, err := c.Store.WriteRange(ctx, repository, NewEntryToValueIterator(it), opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("writing range from entry iterator: %w", err)
	}

	stagingToken := params.StagingToken
	skipped := it.GetSkippedEntries()
	if len(skipped) > 0 {
		c.log(ctx).Warning("Skipped count:", len(skipped))
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

func (c *Catalog) WriteMetaRange(ctx context.Context, repositoryID string, ranges []*graveler.RangeInfo, opts ...graveler.SetOptionsFunc) (*graveler.MetaRangeInfo, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return c.Store.WriteMetaRange(ctx, repository, ranges, opts...)
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
	if repository.ReadOnly {
		return graveler.ErrReadOnlyRepository
	}
	return c.Store.SetGarbageCollectionRules(ctx, repository, rules)
}

func (c *Catalog) GetBranchProtectionRules(ctx context.Context, repositoryID string) (*graveler.BranchProtectionRules, *string, error) {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, nil, err
	}

	return c.Store.GetBranchProtectionRules(ctx, repository)
}

func (c *Catalog) SetBranchProtectionRules(ctx context.Context, repositoryID string, rules *graveler.BranchProtectionRules, lastKnownChecksum *string) error {
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	if repository.ReadOnly {
		return graveler.ErrReadOnlyRepository
	}
	return c.Store.SetBranchProtectionRules(ctx, repository, rules, lastKnownChecksum)
}

func (c *Catalog) PrepareExpiredCommits(ctx context.Context, repositoryID string) (*graveler.GarbageCollectionRunMetadata, error) {
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	if repository.ReadOnly {
		return nil, graveler.ErrReadOnlyRepository
	}
	return c.Store.SaveGarbageCollectionCommits(ctx, repository)
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

func (c *Catalog) uploadFile(ctx context.Context, repo *graveler.RepositoryRecord, location string, fd *os.File, size int64) (string, error) {
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
		StorageID:        repo.StorageID.String(),
		StorageNamespace: repo.StorageNamespace.String(),
		Identifier:       identifier,
		IdentifierType:   block.IdentifierTypeFull,
	}
	_, err = c.BlockAdapter.Put(ctx, obj, size, fd, block.PutOpts{})
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
	if repository.ReadOnly {
		return nil, graveler.ErrReadOnlyRepository
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
			c.log(ctx).WithField("filename", fd.Name()).Warn("Failed to delete temporary gc uncommitted data file")
		}
	}()

	uw := NewUncommittedWriter(fd)

	// Write parquet to local storage
	newMark, hasData, err := gcWriteUncommitted(ctx, c.Store, repository, uw, mark, runID, c.UGCPrepareMaxFileSize, c.UGCPrepareInterval)
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

		name, err = c.uploadFile(ctx, repository, uncommittedLocation, fd, uw.Size())
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
// if replaceSrcMetadata is true, the metadata will be replaced with the provided metadata.
// if replaceSrcMetadata is false, the metadata will be copied from the source entry.
func (c *Catalog) CopyEntry(ctx context.Context, srcRepository, srcRef, srcPath, destRepository, destBranch, destPath string, replaceSrcMetadata bool, metadata Metadata, opts ...graveler.SetOptionsFunc) (*DBEntry, error) {
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

		if srcRepo.StorageID != destRepo.StorageID {
			return nil, fmt.Errorf("%w: cannot copy between repos with different StorageIDs", graveler.ErrInvalidStorageID)
		}
	}

	// copy data to a new physical address
	dstEntry := *srcEntry
	dstEntry.Path = destPath
	dstEntry.AddressType = AddressTypeRelative
	dstEntry.PhysicalAddress = c.PathProvider.NewPath()

	if replaceSrcMetadata {
		dstEntry.Metadata = metadata
	} else {
		dstEntry.Metadata = srcEntry.Metadata
	}

	srcObject := block.ObjectPointer{
		StorageID:        srcRepo.StorageID,
		StorageNamespace: srcRepo.StorageNamespace,
		IdentifierType:   srcEntry.AddressType.ToIdentifierType(),
		Identifier:       srcEntry.PhysicalAddress,
	}
	destObj := block.ObjectPointer{
		StorageID:        destRepo.StorageID,
		StorageNamespace: destRepo.StorageNamespace,
		IdentifierType:   dstEntry.AddressType.ToIdentifierType(),
		Identifier:       dstEntry.PhysicalAddress,
	}
	err = c.BlockAdapter.Copy(ctx, srcObject, destObj)
	if err != nil {
		return nil, err
	}

	// Update creation date only after actual copy!!!
	// The actual file upload can take a while and depend on many factors so we would like
	// The mtime (creationDate) in lakeFS to be as close as possible to the mtime in the underlying storage
	dstEntry.CreationDate = time.Now()

	// create entry for the final copy
	err = c.CreateEntry(ctx, destRepository, destBranch, dstEntry, opts...)
	if err != nil {
		return nil, err
	}
	return &dstEntry, nil
}

func (c *Catalog) DeleteExpiredImports(ctx context.Context) {
	repos, err := c.listRepositoriesHelper(ctx)
	if err != nil {
		c.log(ctx).WithError(err).Warn("Delete expired imports: failed to list repositories")
		return
	}

	for _, repo := range repos {
		err = c.Store.DeleteExpiredImports(ctx, repo)
		if err != nil {
			c.log(ctx).WithError(err).WithField("repository", repo.RepositoryID).Warn("Delete expired imports failed")
		}
	}
}

func (c *Catalog) DeleteExpiredTasks(ctx context.Context) {
	repos, err := c.listRepositoriesHelper(ctx)
	if err != nil {
		c.log(ctx).WithError(err).Warn("Delete expired tasks, failed to list repositories")
		return
	}

	for _, repo := range repos {
		err := c.deleteRepositoryExpiredTasks(ctx, repo)
		if err != nil {
			c.log(ctx).WithError(err).WithField("repository", repo.RepositoryID).Warn("Delete expired tasks failed")
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

func getHashSum(value, signingKey []byte) []byte {
	// create a new HMAC by defining the hash type and the key
	h := hmac.New(sha256.New, signingKey)
	// compute the HMAC
	h.Write(value)
	return h.Sum(nil)
}

func (c *Catalog) VerifyLinkAddress(repository, branch, path, physicalAddress string) error {
	idx := strings.LastIndex(physicalAddress, LinkAddressSigningDelimiter)
	if idx < 0 {
		return fmt.Errorf("address is not signed: %w", graveler.ErrLinkAddressInvalid)
	}
	address := physicalAddress[:idx]
	signature := physicalAddress[idx+1:]

	stringToVerify, err := getAddressJSON(repository, branch, path, address)
	if err != nil {
		return fmt.Errorf("failed json encoding: %w", graveler.ErrLinkAddressInvalid)
	}
	decodedSig, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return fmt.Errorf("malformed address signature: %s: %w", stringToVerify, graveler.ErrLinkAddressInvalid)
	}

	calculated := getHashSum(stringToVerify, []byte(c.signingKey))
	if !hmac.Equal(calculated, decodedSig) {
		return fmt.Errorf("invalid address signature: %w", block.ErrInvalidAddress)
	}
	creationTime, err := c.PathProvider.ResolvePathTime(address)
	if err != nil {
		return err
	}

	if time.Since(creationTime) > LinkAddressTime {
		return graveler.ErrLinkAddressExpired
	}
	return nil
}

func (c *Catalog) signAddress(logicalAddress []byte) string {
	dataHmac := getHashSum(logicalAddress, []byte(c.signingKey))
	return base64.RawURLEncoding.EncodeToString(dataHmac) // Using url encoding to avoid "/"
}

func getAddressJSON(repository, branch, path, physicalAddress string) ([]byte, error) {
	return json.Marshal(struct {
		Repository      string
		Branch          string
		Path            string
		PhysicalAddress string
	}{
		Repository:      repository,
		Branch:          branch,
		Path:            path,
		PhysicalAddress: physicalAddress,
	})
}

func (c *Catalog) GetAddressWithSignature(repository, branch, path string) (string, error) {
	physicalPath := c.PathProvider.NewPath()
	data, err := getAddressJSON(repository, branch, path, physicalPath)
	if err != nil {
		return "", err
	}
	return physicalPath + LinkAddressSigningDelimiter + c.signAddress(data), nil
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
	if c.deleteSensor != nil {
		c.deleteSensor.Close()
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

func (c *Catalog) GetPullRequest(ctx context.Context, repositoryID string, pullRequestID string) (*graveler.PullRequest, error) {
	pid := graveler.PullRequestID(pullRequestID)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "pullRequestID", Value: pid, Fn: graveler.ValidatePullRequestID},
	}); err != nil {
		return nil, err
	}

	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}

	pr, err := c.Store.GetPullRequest(ctx, repository, pid)
	if err != nil {
		return nil, err
	}

	return pr, nil
}

func (c *Catalog) CreatePullRequest(ctx context.Context, repositoryID string, request *PullRequest) (string, error) {
	srcBranchID := graveler.BranchID(request.SourceBranch)
	destBranchID := graveler.BranchID(request.DestinationBranch)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "dest", Value: destBranchID, Fn: graveler.ValidateBranchID},
		{Name: "src", Value: srcBranchID, Fn: graveler.ValidateBranchID},
	}); err != nil {
		return "", err
	}

	// Verify src and dst are different
	if srcBranchID == destBranchID {
		return "", fmt.Errorf("source and destination branches are the same: %w", graveler.ErrSameBranch)
	}

	// Check all entities exist
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}
	if _, err = c.Store.GetBranch(ctx, repository, srcBranchID); err != nil {
		return "", err
	}
	if _, err = c.Store.GetBranch(ctx, repository, destBranchID); err != nil {
		return "", err
	}

	pullID := graveler.NewRunID()
	pull := &graveler.PullRequestRecord{
		ID: graveler.PullRequestID(pullID),
		PullRequest: graveler.PullRequest{
			CreationDate: time.Now(),
			Title:        request.Title,
			Author:       request.Author,
			Description:  request.Description,
			Source:       request.SourceBranch,
			Destination:  request.DestinationBranch,
		},
	}
	if err = c.Store.CreatePullRequest(ctx, repository, pull); err != nil {
		return "", err
	}

	return pullID, nil
}

func shouldSkipByStatus(requested string, status graveler.PullRequestStatus) bool {
	if status.String() == requested {
		return false
	}

	switch requested {
	case graveler.PullRequestStatus_CLOSED.String(): // CLOSED can be either CLOSED OR MERGED
		return status != graveler.PullRequestStatus_CLOSED && status != graveler.PullRequestStatus_MERGED
	case graveler.PullRequestStatus_OPEN.String(): // OPEN must be equal to OPEN
		return status != graveler.PullRequestStatus_OPEN
	default: // Anything else should return all
		return false
	}
}

func (c *Catalog) ListPullRequest(ctx context.Context, repositoryID, prefix string, limit int, after, status string) ([]*PullRequest, bool, error) {
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
	if limit < 0 || limit > ListPullsLimitMax {
		limit = ListPullsLimitMax
	}
	it, err := c.Store.ListPullRequests(ctx, repository)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()

	afterPR := graveler.PullRequestID(after)
	prefixPR := graveler.PullRequestID(prefix)
	if afterPR < prefixPR {
		it.SeekGE(prefixPR)
	} else {
		it.SeekGE(afterPR)
	}
	var pulls []*PullRequest
	for it.Next() {
		v := it.Value()
		if v.ID == afterPR || shouldSkipByStatus(status, v.Status) {
			continue
		}
		pullID := v.ID.String()
		// break in case we got to a pull outside our prefix
		if !strings.HasPrefix(pullID, prefix) {
			break
		}
		p := &PullRequest{
			ID:                pullID,
			Title:             v.Title,
			Status:            strings.ToLower(v.Status.String()),
			Description:       v.Description,
			Author:            v.Author,
			SourceBranch:      v.Source,
			DestinationBranch: v.Destination,
			CreationDate:      v.CreationDate,
			ClosedDate:        v.ClosedDate,
		}
		pulls = append(pulls, p)
		if len(pulls) >= limit+1 {
			break
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	// return results (optionally trimmed) and hasMore
	hasMore := false
	if len(pulls) > limit {
		hasMore = true
		pulls = pulls[:limit]
	}
	return pulls, hasMore, nil
}

func (c *Catalog) UpdatePullRequest(ctx context.Context, repositoryID string, pullRequestID string, request *graveler.UpdatePullRequest) error {
	pullID := graveler.PullRequestID(pullRequestID)
	if err := validator.Validate([]validator.ValidateArg{
		{Name: "repository", Value: repositoryID, Fn: graveler.ValidateRepositoryID},
		{Name: "pullRequestID", Value: pullID, Fn: graveler.ValidatePullRequestID},
	}); err != nil {
		return err
	}
	repository, err := c.getRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	return c.Store.UpdatePullRequest(ctx, repository, pullID, request)
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
