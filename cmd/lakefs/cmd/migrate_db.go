package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/catalog/rocks"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog/migrate"
	"github.com/treeverse/lakefs/db"
)

// migrateDBCmd represents the db command
var migrateDBCmd = &cobra.Command{
	Use:   "db",
	Short: "Migrate MVCC based lakeFS database",
	Long:  `Migrate database content from MVCC model to the current format`,
	Run: func(cmd *cobra.Command, args []string) {
		dbParams := cfg.GetDatabaseParams()
		err := db.ValidateSchemaUpToDate(dbParams)
		if errors.Is(err, db.ErrSchemaNotCompatible) {
			fmt.Println("Migration version mismatch, for more information see https://docs.lakefs.io/deploying/upgrade.html")
			os.Exit(1)
		}
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		dbPool := db.BuildDatabaseConnection(dbParams)
		defer dbPool.Close()

		entryCatalog, err := rocks.NewEntryCatalog(cfg, dbPool)
		if err != nil {
			fmt.Println("Error during initialize entry catalog", err)
			os.Exit(1)
		}

		mvccParams := cfg.GetMvccCatalogerCatalogParams()
		mvccCataloger := mvcc.NewCataloger(dbPool, mvcc.WithParams(mvccParams))

		migrateTool, err := migrate.NewMigrate(dbPool, entryCatalog, mvccCataloger)
		if err != nil {
			fmt.Println("Failed to create a new migrate:", err)
			os.Exit(1)
		}

		repoExpr, _ := cmd.Flags().GetString("repository")
		if err := migrateTool.FilterRepository(repoExpr); err != nil {
			fmt.Println("Failed to setup repository filter:", err)
			os.Exit(1)
		}

		err = migrateTool.Run()
		if err != nil {
			fmt.Println("Migration failed")
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("Migrate completed")
	},
}

//nolint:gochecknoinits
func init() {
	migrateCmd.AddCommand(migrateDBCmd)
	migrateDBCmd.Flags().String("repository", "", "Repository filter (regexp)")
}
