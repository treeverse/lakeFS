package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
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
		logger := logging.Default()
		cfg := loadConfig()

		ctx := cmd.Context()
		kvParams := cfg.GetKVParams()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			logger.WithError(err).Fatal("Failed to open KV store")
		}
		defer kvStore.Close()

		version, err := kv.GetDBSchemaVersion(ctx, kvStore)
		if err != nil {
			logger.WithError(err).Fatal("Failed to get DB schema version")
			return

		}
		logger.WithField("schema_version", version).Info("DB schema version")
		fmt.Printf("Database schema version: %d\n", version)
	},
}

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Apply all up migrations",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("No migrations to apply.\n")
	},
}

var gotoCmd = &cobra.Command{
	Use:   "goto",
	Short: "Migrate to version V.",
	Run: func(cmd *cobra.Command, args []string) {
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
	_ = gotoCmd.Flags().Bool("force", false, "force migrate")
}
