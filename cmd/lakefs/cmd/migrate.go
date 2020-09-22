package cmd

import (
	"fmt"
	"os"

	"github.com/treeverse/lakefs/db"

	"github.com/spf13/cobra"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "manage migrations",
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print current migration version and available version",
	Run: func(cmd *cobra.Command, args []string) {
		version, _, err := db.MigrateVersion(cfg.GetDatabaseParams())
		if err != nil {
			fmt.Printf("Failed to get info for schema: %s\n", err)
			return
		}
		available, err := db.GetLastMigrationAvailable(0)
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
		err := db.MigrateUp(cfg.GetDatabaseParams())
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}

	},
}

var downCmd = &cobra.Command{
	Use:   "down",
	Short: "Apply all down migrations",
	Run: func(cmd *cobra.Command, args []string) {
		err := db.MigrateDown(cfg.GetDatabaseParams())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

var gotoCmd = &cobra.Command{
	Use:   "goto",
	Short: "Migrate to version V.",
	Run: func(cmd *cobra.Command, args []string) {
		version, err := cmd.Flags().GetUint("version")
		if err != nil {
			fmt.Printf("Failed to get value for 'version': %s\n", err)
			os.Exit(1)
		}
		uri := cfg.GetDatabaseParams()
		err = db.MigrateTo(uri, version)
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
	migrateCmd.AddCommand(downCmd)
	migrateCmd.AddCommand(gotoCmd)
	_ = gotoCmd.Flags().Uint("version", 0, "version number")
	_ = gotoCmd.MarkFlagRequired("version")
}
