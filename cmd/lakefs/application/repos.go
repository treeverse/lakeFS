package application

import (
	"context"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"net/url"
)

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
func CheckRepos(lakeFsCmd LakeFsCmdContext, authService *AuthService, blockStore *BlockStore, c *catalog.Catalog) {
	logger := lakeFsCmd.logger
	ctx := lakeFsCmd.ctx
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
