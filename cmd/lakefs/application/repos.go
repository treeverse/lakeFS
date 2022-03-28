package application

import (
	"context"
	"errors"
	"fmt"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/uri"
	"net/url"
	"strings"
)

const (
	ManifestURLFormat = "s3://example-bucket/inventory/YYYY-MM-DDT00-00Z/manifest.json"
)

var errInvalidManifestURL = errors.New("invalid manifest url")

// checkMetadataPrefix checks for non-migrated repos of issue #2397 (https://github.com/treeverse/lakeFS/issues/2397)
func checkMetadataPrefix(ctx context.Context, repo *catalog.Repository, logger logging.Logger, adapter block.Adapter, repoStorageType block.StorageType) {
	if repoStorageType != block.StorageTypeGS &&
		repoStorageType != block.StorageTypeAzure {
		return
	}

	const dummyFile = "dummy"
	if _, err := adapter.Get(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		Identifier:       dummyFile,
	}, -1); err != nil {
		logger.WithFields(logging.Fields{
			"path":              dummyFile,
			"storage_namespace": repo.StorageNamespace,
		}).Fatal("Can't find dummy file in storage namespace, did you run the migration? " +
			"(https://docs.lakefs.io/reference/upgrade.html#data-migration-for-version-v0500)")
	}
}

// checkRepos iterates on all repos and validates that their settings are correct.
func CheckRepos(lakeFsCmdCtx LakeFsCmdContext, authService *AuthService, blockStore *BlockStore, c *catalog.Catalog) {
	logger := lakeFsCmdCtx.logger
	ctx := lakeFsCmdCtx.ctx
	initialized, err := authService.authMetadataManager.IsInitialized(ctx)
	if err != nil {
		logger.WithError(err).Fatal("Failed to check if lakeFS is initialized")
	}
	if !initialized {
		logger.Debug("lakeFS isn't initialized, skipping mismatched adapter checks")
	} else {
		logger.
			WithField("adapter_type", blockStore.blockAdapter.BlockstoreType()).
			Debug("lakeFS is initialized, checking repositories for mismatched adapter")
		hasMore := true
		next := ""

		for hasMore {
			var err error
			var repos []*catalog.Repository
			repos, hasMore, err = c.ListRepositories(ctx, -1, "", next)
			if err != nil {
				logger.WithError(err).Fatal("Checking existing repositories failed")
			}

			adapterStorageType := blockStore.blockAdapter.BlockstoreType()
			for _, repo := range repos {
				nsURL, err := url.Parse(repo.StorageNamespace)
				if err != nil {
					logger.WithError(err).Fatalf("Failed to parse repository %s namespace '%s'", repo.Name, repo.StorageNamespace)
				}
				repoStorageType, err := block.GetStorageType(nsURL)
				if err != nil {
					logger.WithError(err).Fatalf("Failed to parse to parse storage type '%s'", nsURL)
				}

				checkForeignRepo(repoStorageType, logger, adapterStorageType, repo.Name)
				checkMetadataPrefix(ctx, repo, logger, blockStore.blockAdapter, repoStorageType)

				next = repo.Name
			}
		}
	}
}

func NewRepository(ctx context.Context, c catalog.Interface, repoName string, manifestURL string) (*catalog.Repository, error) {
	u := uri.Must(uri.Parse(repoName))
	if !u.IsRepository() {
		return nil, uri.ErrInvalidRefURI
	}
	validRepoName := u.Repository
	parsedURL, err := url.Parse(manifestURL)
	if err != nil || parsedURL.Scheme != "s3" || !strings.HasSuffix(parsedURL.Path, "/manifest.json") {
		return nil, fmt.Errorf("%w: expected format %s", errInvalidManifestURL, ManifestURLFormat)
	}
	repo, err := c.GetRepository(ctx, validRepoName)
	if err != nil {
		// TODO: this error looks weird
		return nil, fmt.Errorf("read repository %s: %w", repoName, err)
	}
	// import branch is created on the fly
	return repo, nil
}

// checkForeignRepo checks whether a repo storage namespace matches the block adapter.
// A foreign repo is a repository which namespace doesn't match the current block adapter.
// A foreign repo might exists if the lakeFS instance configuration changed after a repository was
// already created. The behaviour of lakeFS for foreign repos is undefined and should be blocked.
func checkForeignRepo(repoStorageType block.StorageType, logger logging.Logger, adapterStorageType, repoName string) {
	if adapterStorageType != repoStorageType.BlockstoreType() {
		logger.Fatalf("Mismatched adapter detected. lakeFS started with adapter of type '%s', but repository '%s' is of type '%s'",
			adapterStorageType, repoName, repoStorageType.BlockstoreType())
	}
}
