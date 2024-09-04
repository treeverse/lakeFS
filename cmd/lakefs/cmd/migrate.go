package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Manage migrations",
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print current migration version and available version",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
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
		cfg := loadConfig()
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

func DoMigration(ctx context.Context, kvStore kv.Store, _ *config.Config, _ bool) error {
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
		cfg := loadConfig()
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
	_ = gotoCmd.Flags().Uint("version", 0, "version number")
	_ = gotoCmd.MarkFlagRequired("version")
	_ = upCmd.Flags().Bool("force", false, "force migrate, otherwise, migration will fail on warnings ")
	_ = gotoCmd.Flags().Bool("force", false, "force migrate")
}
