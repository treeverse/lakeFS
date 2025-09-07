package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
	blockfactory "github.com/treeverse/lakefs/modules/block/factory"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
)

// Task represents a migration task
type Task struct {
	Description string
	Function    func(context.Context, *TaskServices, bool) error
}

type TaskServices struct {
	Config       config.Config
	KVStore      kv.Store
	BlockAdapter block.Adapter
}

// taskRegistry - migration tasks registry
var taskRegistry = map[string]Task{
	"storage-ns-encoded": {
		Description: "Fix storage namespace encoded path - parse and update repositories with encoded storage namespaces",
		Function:    storageNamespaceEncodedTask,
	},
}

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Manage migrations",
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print current migration version and available version",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := LoadConfig().GetBaseConfig()
		kvParams, err := kvparams.NewConfig(&cfg.Database)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		ctx := cmd.Context()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		version := mustValidateSchemaVersion(ctx, kvStore)
		fmt.Printf("Database schema version: %d\n", version)
	},
}

func mustValidateSchemaVersion(ctx context.Context, kvStore kv.Store) int {
	version, err := kv.ValidateSchemaVersion(ctx, kvStore)
	switch {
	case err == nil:
		return version
	case errors.Is(err, kv.ErrNotFound):
		_, _ = fmt.Fprintf(os.Stderr, "No version information - KV not initialized.\n")
	case errors.Is(err, kv.ErrMigrationVersion),
		errors.Is(err, kv.ErrMigrationRequired):
		_, _ = fmt.Fprintf(os.Stderr, "Schema version: %d. %s\n", version, err)
	case err != nil:
		_, _ = fmt.Fprintf(os.Stderr, "Failed to get schema version: %s\n", err)
	}

	os.Exit(1)
	return -1
}

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Apply all up migrations",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := LoadConfig().GetBaseConfig()
		kvParams, err := kvparams.NewConfig(&cfg.Database)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		ctx := cmd.Context()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		force, _ := cmd.Flags().GetBool("force")

		_, err = kv.ValidateSchemaVersion(ctx, kvStore)
		switch {
		case err == nil:
			fmt.Printf("No migrations to apply.\n")
		case errors.Is(err, kv.ErrMigrationVersion):
			_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		case errors.Is(err, kv.ErrMigrationRequired):
			err = DoMigration(ctx, kvStore, cfg, force)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Migration failed: %s\n", err)
				os.Exit(1)
			}
			fmt.Printf("Migration completed successfully.\n")
		default:
			_, _ = fmt.Fprintf(os.Stderr, "Failed to get KV version: %s\n", err)
			os.Exit(1)
		}
	},
}

func DoMigration(ctx context.Context, kvStore kv.Store, _ *config.BaseConfig, _ bool) error {
	var (
		version int
		err     error
	)

	// This loop performs all migration paths until reaching the latest KV schema version
	// A precondition is coming up from a lakeFS version that supports KV migration (kv.InitialMigrateVersion)
	// Each migration scenario is responsible also to bump the KV schema to the next version
	// The iteration ends when we reach the latest version
	for !kv.IsLatestSchemaVersion(version) {
		version, err = kv.GetDBSchemaVersion(ctx, kvStore)
		if err != nil {
			return err
		}
		switch {
		case version >= kv.NextSchemaVersion || version < kv.InitialMigrateVersion:
			return fmt.Errorf("wrong starting version %d: %w", version, kv.ErrMigrationVersion)
		// Due to removal of ACLs from lakeFS, ACL migration code is no longer needed. Users who previously used RBAC will be migrated
		// directly to basic auth.
		// Since there are no other migration scenarios ATM we just need to bump the schema version
		// This will need to be modified once a new migration scenario is required
		default:
			err = kv.SetDBSchemaVersion(ctx, kvStore, kv.ACLImportMigrateVersion)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

var gotoCmd = &cobra.Command{
	Use:   "goto",
	Short: "Migrate to version V.",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := LoadConfig().GetBaseConfig()
		kvParams, err := kvparams.NewConfig(&cfg.Database)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		ctx := cmd.Context()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		_ = mustValidateSchemaVersion(ctx, kvStore)
		fmt.Printf("No migrations to apply.\n")
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(migrateCmd)
	migrateCmd.AddCommand(versionCmd)
	migrateCmd.AddCommand(upCmd)
	migrateCmd.AddCommand(gotoCmd)
	migrateTaskCmd.AddCommand(migrateTaskListCmd)
	_ = migrateTaskRunCmd.Flags().Bool("dry-run", false, "dry-run mode - show what would be changed without making actual changes")
	migrateTaskCmd.AddCommand(migrateTaskRunCmd)
	migrateCmd.AddCommand(migrateTaskCmd)
	_ = gotoCmd.Flags().Uint("version", 0, "version number")
	_ = gotoCmd.MarkFlagRequired("version")
	_ = upCmd.Flags().Bool("force", false, "force migrate, otherwise, migration will fail on warnings ")
	_ = gotoCmd.Flags().Bool("force", false, "force migrate")
}

var migrateTaskCmd = &cobra.Command{
	Use:   "task",
	Short: "Manage migration tasks",
}

var migrateTaskListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available migration tasks",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Available migration tasks:")
		for name, task := range taskRegistry {
			fmt.Printf("  %s: %s\n", name, task.Description)
		}
	},
}

var migrateTaskRunCmd = &cobra.Command{
	Use:   "run [task-name]",
	Short: "Run a specific migration task",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		taskName := args[0]
		task, exists := taskRegistry[taskName]
		if !exists {
			_, _ = fmt.Fprintf(os.Stderr, "Task '%s' not found\n", taskName)
			os.Exit(1)
		}

		cfg := LoadConfig()
		baseConfig := cfg.GetBaseConfig()
		kvParams, err := kvparams.NewConfig(&baseConfig.Database)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		ctx := cmd.Context()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		// init block store
		blockAdapter, err := blockfactory.BuildBlockAdapter(ctx, nil, cfg)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to create block adapter: %s\n", err)
			os.Exit(1)
		}

		dryRun, _ := cmd.Flags().GetBool("dry-run")
		fmt.Printf("Running task: %s", taskName)
		if dryRun {
			fmt.Printf(" (dry-run mode)")
		}
		fmt.Println()

		services := &TaskServices{
			Config:       cfg,
			KVStore:      kvStore,
			BlockAdapter: blockAdapter,
		}
		err = task.Function(ctx, services, dryRun)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Task failed: %s\n", err)
			os.Exit(1)
		}
		fmt.Printf("Task '%s' completed successfully\n", taskName)
	},
}

func storageNamespaceEncodedTask(ctx context.Context, services *TaskServices, dryRun bool) error {
	kvStore := services.KVStore
	iter, err := ref.NewRepositoryIterator(ctx, kvStore, services.Config.StorageConfig())
	if err != nil {
		return fmt.Errorf("new repository iterator: %w", err)
	}
	defer iter.Close()

	encodedRepoCount := 0
	updatedCount := 0
	for iter.Next() {
		repo := iter.Value()
		if repo == nil {
			continue
		}

		// quick check - skip if no '%' in storage namespace
		originalNS := string(repo.StorageNamespace)
		if !strings.Contains(originalNS, "%") {
			continue
		}
		// Parse the storage namespace the old way
		parsedNS, err := GetOldParsedStorageNamespace(originalNS)
		if err != nil {
			return fmt.Errorf("parse storage namespace %s for repository %s: %w", originalNS, repo.RepositoryID, err)
		}
		if parsedNS == originalNS {
			continue
		}

		encodedRepoCount++
		fmt.Printf("Repository %s: storage namespace was encoded\n", repo.RepositoryID)
		fmt.Printf("  Original: %s\n", originalNS)
		fmt.Printf("  Resolved: %s\n", parsedNS)

		// Verify that the unescaped path works by getting the dummy file
		fmt.Printf("  Verifying block adapter access to resolved namespace...\n")
		if err := verifyStorageNamespaceAccess(ctx, services, string(repo.StorageID), parsedNS); err != nil {
			fmt.Printf("  Skipping update for repository %s - the unescaped path is not accessible\n", repo.RepositoryID)
			continue
		}
		fmt.Printf("  Block adapter access verification successful\n")

		if !dryRun {
			// Update the repository with the resolved namespace
			repo.StorageNamespace = graveler.StorageNamespace(parsedNS)
			err = kv.SetMsg(ctx, kvStore, graveler.RepositoriesPartition(), []byte(graveler.RepoPath(repo.RepositoryID)), graveler.ProtoFromRepo(repo))
			if err != nil {
				return fmt.Errorf("failed to update repository %s: %w", repo.RepositoryID, err)
			}
			fmt.Printf("  Updated to: %s\n", parsedNS)
		} else {
			fmt.Printf("  Would update to: %s\n", parsedNS)
		}
		updatedCount++
	}

	if err = iter.Err(); err != nil {
		return fmt.Errorf("iterate repositories: %w", err)
	}

	fmt.Printf("\nSummary: Found %d repositories with encoded storage namespaces\n", encodedRepoCount)
	if encodedRepoCount > updatedCount {
		skippedCount := encodedRepoCount - updatedCount
		fmt.Printf("Skipped %d repositories due to block adapter verification failures\n", skippedCount)
	}
	if updatedCount > 0 && dryRun {
		fmt.Printf("Note: This was a dry-run.\n")
	} else if updatedCount > 0 {
		fmt.Printf("Successfully updated %d repositories.\n", updatedCount)
	}

	return nil
}

// verifyStorageNamespaceAccess tries to access a dummy object in the given storage namespace.
func verifyStorageNamespaceAccess(ctx context.Context, taskServices *TaskServices, storageID string, storageNamespace string) error {
	// Use a dummy object path that should always exist if the namespace is correct
	prefix := taskServices.Config.GetBaseConfig().Committed.BlockStoragePrefix
	objName := strings.TrimPrefix(prefix+"/dummy", "/")

	obj := block.ObjectPointer{
		StorageID:        storageID,
		StorageNamespace: storageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       objName,
	}
	s, err := taskServices.BlockAdapter.Get(ctx, obj)
	if err != nil {
		return fmt.Errorf("get dummy object: %w", err)
	}
	return s.Close()
}

func GetOldParsedStorageNamespace(ns string) (string, error) {
	// Parse the storage namespace URL, using our default resolver
	parsedURI, err := url.ParseRequestURI(ns)
	if err != nil {
		return "", err
	}

	oldPath := strings.TrimPrefix(parsedURI.Path, "/")
	var result string
	if len(parsedURI.Host) == 0 {
		result = parsedURI.Scheme + "://" + oldPath
	} else {
		result = parsedURI.Scheme + "://" + parsedURI.Host + "/" + oldPath
	}
	return result, nil
}
