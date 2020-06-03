package cmd

import (
	"fmt"
	"os"

	"github.com/treeverse/lakefs/config"

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
		for name, key := range config.SchemaDBKeys {
			version, _, err := db.MigrateVersion(name, cfg.GetDatabaseURI(key))
			if err != nil {
				fmt.Printf("Failed to get info for schema: %s.\n%s\n", name, err)
				continue
			}
			available, err := db.GetLastMigrationAvailable(name, version)
			if err != nil {
				fmt.Printf("Failed to get info for schema: %s.\n%s\n", name, err)
				continue
			}
			fmt.Printf("%s version:%d  available:%d\n", name, version, available)
		}
	},
}

var upCmd = &cobra.Command{
	Use:   "up",
	Short: " Apply all up migrations",
	Run: func(cmd *cobra.Command, args []string) {
		for name, key := range config.SchemaDBKeys {
			err := db.MigrateUp(name, cfg.GetDatabaseURI(key))
			if err != nil {

				fmt.Printf("Failed to setup DB: %s\n", err)
				os.Exit(1)
			}
		}
	},
}

var downCmd = &cobra.Command{
	Use:   "down",
	Short: "Apply all down migrations",
	Run: func(cmd *cobra.Command, args []string) {
		for name, key := range config.SchemaDBKeys {
			err := db.MigrateDown(name, cfg.GetDatabaseURI(key))
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
	},
}

var gotoCmd = &cobra.Command{
	Use:   "goto",
	Short: "Migrate to version V.",
	Run: func(cmd *cobra.Command, args []string) {
		schemaName, err := cmd.Flags().GetString("schema")
		version, err := cmd.Flags().GetUint("version")
		uri := cfg.GetDatabaseURI(config.SchemaDBKeys[schemaName])
		if len(uri) == 0 {
			fmt.Printf("Unknown schema: %s\n", schemaName)
			os.Exit(1)
		}
		err = db.MigrateTo(schemaName, uri, version)
		if err != nil {
			fmt.Printf("Failed to migrate schema: %s to version %d.\n%s\n", schemaName, version, err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
	migrateCmd.AddCommand(versionCmd)
	migrateCmd.AddCommand(upCmd)
	migrateCmd.AddCommand(downCmd)
	migrateCmd.AddCommand(gotoCmd)
	_ = gotoCmd.Flags().String("schema", "", "schema name")
	_ = gotoCmd.MarkFlagRequired("schema")
	_ = gotoCmd.Flags().Uint("version", 0, "version number")
	_ = gotoCmd.MarkFlagRequired("version")
}
