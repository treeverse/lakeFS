package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/migrations"
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
		kvParams, err := cfg.DatabaseParams()
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
	if errors.Is(err, kv.ErrNotFound) {
		_, _ = fmt.Fprintf(os.Stderr, "No version information - KV not initialized.\n")
		os.Exit(1)
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to get schema version: %s\n", err)
		os.Exit(1)
	}
	return version
}

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Apply all up migrations",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		kvParams, err := cfg.DatabaseParams()
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

		var version int
		for version < kv.LatestVersion {
			version = mustValidateSchemaVersion(ctx, kvStore)
			switch {
			case version < kv.ACLNoReposMigrateVersion:
				_, _ = fmt.Fprintf(os.Stderr, "Migration to ACL required. Did you migrate using version v0.99.x? https://docs.lakefs.io/reference/access-control-list.html#migrating-from-the-previous-version-of-acls")
				os.Exit(1)

			case version < kv.ACLImportMigrateVersion:
				// skip migrate to ACL for users with External authorizations
				if cfg.IsAuthTypeAPI() && !cfg.IsAuthUISimplified() {
					fmt.Println("skipping ACL migration - external Authorization")
				} else {
					if err = migrations.MigrateImportPermissions(ctx, kvStore); err != nil {
						_, _ = fmt.Fprintf(os.Stderr, "Migration failed: %s\n", err)
						os.Exit(1)
					}
					fmt.Printf("Migration completed successfully.\n")
					return
				}
			}
		}
		fmt.Printf("No migrations to apply.\n")
	},
}

var gotoCmd = &cobra.Command{
	Use:   "goto",
	Short: "Migrate to version V.",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		kvParams, err := cfg.DatabaseParams()
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
