package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
)

// setupdbCmd represents the setupdb command
var setupdbCmd = &cobra.Command{
	Use:   "setupdb",
	Short: "Run schema and data migrations on a fresh database",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		migrator := db.NewDatabaseMigrator()
		for name, key := range config.SchemaDBKeys {
			migrator.AddDB(name, cfg.GetDatabaseURI(key))
		}
		err := migrator.Migrate(ctx)
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(setupdbCmd)
}
