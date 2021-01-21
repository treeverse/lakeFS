package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog/migrate"
	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/catalog/rocks"
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
		migrateTool.SetReporter(&migrateReporter{})
		repoExpr, _ := cmd.Flags().GetString("repository")
		if err := migrateTool.FilterRepository(repoExpr); err != nil {
			fmt.Println("Failed to setup repository filter:", err)
			os.Exit(1)
		}

		ctx := context.Background()
		err = migrateTool.Run(ctx)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("Migrate completed")
	},
}

type migrateReporter struct{}

func (m *migrateReporter) BeginRepository(repository string) {
	fmt.Println("Migrate repository:", repository)
}

func (m *migrateReporter) BeginCommit(ref, message, committer, branch string) {
	fmt.Printf("  Commit: %s (%s) [%s] <%s>\n", message, ref, committer, branch)
}

func (m *migrateReporter) EndRepository(err error) {
	if err != nil {
		fmt.Println("Failed to migrate repository:", err)
	}
}

//nolint:gochecknoinits
func init() {
	migrateCmd.AddCommand(migrateDBCmd)
	migrateDBCmd.Flags().String("repository", "", "Repository filter (regexp)")
}
