package cmd

import (
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
		migrator := db.NewDatabaseMigrator().
			AddDB(config.SchemaMetadata, cfg.ConnectMetadataDatabase()).
			AddDB(config.SchemaAuth, cfg.ConnectAuthDatabase())
		err := migrator.Migrate()
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(setupdbCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// setupdbCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// setupdbCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
