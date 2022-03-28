package cmd

import (
	"fmt"
	"github.com/treeverse/lakefs/cmd/lakefs/application"
	"github.com/treeverse/lakefs/pkg/logging"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/db"
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
		logger := logging.FromContext(ctx)
		lakeFsCmdCtx := application.NewLakeFsCmdContext(ctx, cfg, logger)
		databaseService := application.NewDatabaseService(lakeFsCmdCtx)
		err := databaseService.Migrate(ctx)
		if err != nil {
			fmt.Printf("Failed to init database service: %s\n", err)
			return
		}
		defer databaseService.Close()
		version, _, err := databaseService.MigrateVersion(ctx)
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
		version, err := cmd.Flags().GetUint("version")
		if err != nil {
			fmt.Printf("Failed to get value for 'version': %s\n", err)
			os.Exit(1)
		}
		force, _ := cmd.Flags().GetBool("force")
		uri := cfg.GetDatabaseParams()
		err = db.MigrateTo(ctx, uri, version, force)
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
