package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	auth_migrate "github.com/treeverse/lakefs/pkg/auth/migrate"
	"github.com/treeverse/lakefs/pkg/kv"
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
		fmt.Fprintf(os.Stderr, "No version information - KV not initialized.\n")
		os.Exit(1)
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to get schema version: %s\n", err)
		os.Exit(1)
	}
	return version
}

var authCmd = &cobra.Command{
	Use:   "auth-acl",
	Short: "Apply RBAC-to-ACL migration",
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.FromContext(cmd.Context()).WithField("phase", "migrate")
		cfg := loadConfig()
		now := time.Now()
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

		authService := auth.NewAuthService(
			kvStore,
			crypt.NewSecretStore(cfg.AuthEncryptionSecret()),
			nil,
			cfg.Auth.Cache,
			logger.WithField("service", "auth_service"),
		)
		reallyUpdate, _ := cmd.Flags().GetBool("yes")
		err = auth_migrate.RBACToACL(cmd.Context(), authService, reallyUpdate, now, func(warn error) {
			fmt.Printf("WARNING: %s", warn.Error())
		})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to upgrade RBAC policies to ACLs: %s\n", err)
			os.Exit(1)
		}
	},
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

		_ = mustValidateSchemaVersion(ctx, kvStore)
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
	migrateCmd.AddCommand(authCmd)
	_ = gotoCmd.Flags().Uint("version", 0, "version number")
	_ = gotoCmd.MarkFlagRequired("version")
	_ = gotoCmd.Flags().Bool("force", false, "force migrate")
}
