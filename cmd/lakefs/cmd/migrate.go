package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
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

func migrateImportACL(ctx context.Context, kvStore kv.Store) error {
	policyKey := model.PolicyPath(acl.ACLPolicyName(acl.ACLWritersGroup))
	p := model.PolicyData{}
	_, err := kv.GetMsg(ctx, kvStore, model.PartitionKey, policyKey, &p)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return nil // No policy - nothing to update
		}
	}

	fmt.Println(p.Acl.Permission)
	p.Statements = append(p.Statements, &model.StatementData{
		Effect:   model.StatementEffectAllow,
		Action:   []string{"fs:Import*"},
		Resource: "*",
	})
	p.CreatedAt = timestamppb.Now()

	if err = kv.SetMsg(ctx, kvStore, model.PartitionKey, policyKey, &p); err != nil {
		return err
	}
	err = kv.SetDBSchemaVersion(ctx, kvStore, kv.ACLImportMigrateVersion)
	if err != nil {
		fmt.Println("migration succeeded - failed to upgrade version, to fix this re-run migration")
	}
	return nil
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

		force, _ := cmd.Flags().GetBool("force")
		// -- migrate ACL import start
		if mustValidateSchemaVersion(ctx, kvStore) < kv.ACLImportMigrateVersion || force {
			// skip migrate to ACL for users with External authorizations
			if cfg.IsAuthTypeAPI() {
				fmt.Println("skipping ACL migration - external Authorization")
			} else {
				if err = migrateImportACL(ctx, kvStore); err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "Migration failed: %s\n", err)
					os.Exit(1)
				}
			}
			fmt.Printf("Migration completed successfully.\n")
		}
		// -- migrate ACL import ends here

		// TODO(niro): return once Migrate to ACL is removed from code
		// _ = mustValidateSchemaVersion(ctx, kvStore)
		// fmt.Printf("No migrations to apply.\n")
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
