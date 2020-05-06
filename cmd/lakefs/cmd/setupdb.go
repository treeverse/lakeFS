package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/db"
)

// setupdbCmd represents the setupdb command
var setupdbCmd = &cobra.Command{
	Use:   "setupdb",
	Short: "Run schema and data migrations on a fresh database",
	RunE: func(cmd *cobra.Command, args []string) error {
		migrator := db.NewDatabaseMigrator().
			AddDB(db.SchemaMetadata, cfg.ConnectMetadataDatabase()).
			AddDB(db.SchemaAuth, cfg.ConnectAuthDatabase())
		return migrator.Migrate()
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
