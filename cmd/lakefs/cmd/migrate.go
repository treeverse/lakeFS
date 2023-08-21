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
	"github.com/treeverse/lakefs/pkg/kv/migrations"
	"github.com/treeverse/lakefs/pkg/logging"
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
		kvParams, err := kvparams.NewConfig(cfg)
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
		kvParams, err := kvparams.NewConfig(cfg)
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

func DoMigration(ctx context.Context, kvStore kv.Store, cfg *config.Config, force bool) error {
	var (
		version int
		err     error
	)
	for !kv.IsLatestSchemaVersion(version) {
		version, err = kv.GetDBSchemaVersion(ctx, kvStore)
		if err != nil {
			return err
		}
		switch {
		case version >= kv.NextSchemaVersion || version < kv.InitialMigrateVersion:
			return fmt.Errorf("wrong starting version %d: %w", version, kv.ErrMigrationVersion)
		case version < kv.ACLNoReposMigrateVersion:
			err = migrations.MigrateToACL(ctx, kvStore, cfg, logging.ContextUnavailable(), version, force)
		case version < kv.ACLImportMigrateVersion:
			err = migrations.MigrateImportPermissions(ctx, kvStore, cfg)
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
		kvParams, err := kvparams.NewConfig(cfg)
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
