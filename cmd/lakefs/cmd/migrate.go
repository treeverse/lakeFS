package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"

	"github.com/spf13/cobra"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "manage migrations",
}

func getSchemas() map[string]string {
	return map[string]string{
		config.SchemaMetadata: cfg.MetadataDatabaseURI(),
		config.SchemaAuth:     cfg.AuthDatabaseURI(),
	}
}

var infoCmd = &cobra.Command{
	Use:   "version",
	Short: "prints the currently active migration version",
	Run: func(cmd *cobra.Command, args []string) {
		for name, uri := range getSchemas() {
			version, _, err := db.MigrateVersion(name, uri)
			if err != nil {
				fmt.Printf("Failed to get info for schema: %s.\n %s\n", name, err)
				os.Exit(1)
			}
			available, err := db.GetLastMigrationAvailable(name, version)
			if err != nil {
				fmt.Printf("Failed to get info for schema: %s.\n %s\n", name, err)
				os.Exit(1)
			}
			fmt.Printf("%s version:%d  available:%d\n", name, version, available)
		}
	},
}

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "MigrateUp looks at the currently active migration version and will migrate all the way up (applying all up migrations).",
	Run: func(cmd *cobra.Command, args []string) {
		for name, uri := range getSchemas() {
			err := db.MigrateUp(name, uri)
			if err != nil {
				fmt.Printf("Failed to setup DB: %s\n", err)
				os.Exit(1)
			}
		}
	},
}

var downCmd = &cobra.Command{
	Use:   "down",
	Short: "MigrateDown looks at the currently active migration version and will migrate all the way down (applying all down migrations).",
	Run: func(cmd *cobra.Command, args []string) {
		for name, uri := range getSchemas() {
			err := db.MigrateDown(name, uri)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
	},
}

var toCmd = &cobra.Command{
	Use:   "goto [version]",
	Short: "Migrate to version V.",
	Args:  ValidationChain(HasNArgs(1)),
	Run: func(cmd *cobra.Command, args []string) {
		schemaName, err := cmd.Flags().GetString("schema")
		if err != nil {
			fmt.Printf("Failed to get schema flag: %s.", err)
			os.Exit(1)
		}

		version, err := strconv.ParseUint(args[0], 10, 32)
		if err != nil {
			fmt.Printf("Failed to get parse verison:%s.  %s\n ", args[0], err)
			os.Exit(1)
		}
		if uri, ok := getSchemas()[schemaName]; ok {
			err = db.MigrateTo(schemaName, uri, uint(version))
			if err != nil {
				fmt.Printf("Failed to migrate schema: %s to version %d.\n %s\n", schemaName, uint(version), err)
				os.Exit(1)
			}
		} else {
			if len(schemaName) > 0 {
				fmt.Printf("unknown schema: %s \n", schemaName)
			} else {
				fmt.Println("please choose a schema (e.g --schema lakefs_index)")
			}
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
	migrateCmd.AddCommand(infoCmd)
	migrateCmd.AddCommand(upCmd)
	migrateCmd.AddCommand(downCmd)
	migrateCmd.AddCommand(toCmd)
	toCmd.Flags().String("schema", "", "choose schema  (default is all)")
}
