package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/kv"
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
		ctx := cmd.Context()
		dbParams := cfg.GetDatabaseParams()
		if dbParams.KVEnabled {
			kvStore, err := kv.Open(ctx, cfg.GetKVParams())
			if err != nil {
				fmt.Printf("Failed to open KV store: %s\n", err)
				return
			}
			defer kvStore.Close()
			if kv.ValidateSchemaVersion(ctx, kvStore, false) == nil {
				// Migration already occurred in KV or setup is required
				return
			}
		}
		dbPool := db.BuildDatabaseConnection(ctx, dbParams)
		defer dbPool.Close()
		version, _, err := db.MigrateVersion(ctx, dbPool, dbParams)
		if err != nil {
			fmt.Printf("Failed to get info for schema: %s\n", err)
			return
		}
		available, err := db.GetLastMigrationAvailable()
		if err != nil {
			fmt.Printf("Failed to get info for schema: %s\n", err)
			return
		}
		fmt.Printf("version:%d  available:%d\n", version, available)
	},
}

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Apply all up migrations",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		err := db.MigrateUp(cfg.GetDatabaseParams())
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}
	},
}

var gotoCmd = &cobra.Command{
	Use:   "goto",
	Short: "Migrate to version V.",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		ctx := cmd.Context()
		dbParams := cfg.GetDatabaseParams()
		if dbParams.KVEnabled {
			fmt.Printf("Unsupported command for KV migration version\n")
			os.Exit(1)
		}
		version, err := cmd.Flags().GetUint("version")
		if err != nil {
			fmt.Printf("Failed to get value for 'version': %s\n", err)
			os.Exit(1)
		}
		force, _ := cmd.Flags().GetBool("force")
		err = db.MigrateTo(ctx, dbParams, version, force)
		if err != nil {
			fmt.Printf("Failed to migrate to version %d.\n%s\n", version, err)
			os.Exit(1)
		}
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
	_ = gotoCmd.Flags().Bool("force", false, "force migrate")
}
