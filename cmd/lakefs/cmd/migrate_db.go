package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/catalog/migrate"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/ident"
)

// migrateDBCmd represents the db command
var migrateDBCmd = &cobra.Command{
	Use:   "commits",
	Short: "Migrate merge commits parents ordering",
	Long:  `Migrate database content (commits, branches and tags) for merge commits, to place the destination parent first`,
	Run: func(cmd *cobra.Command, args []string) {
		dbParams := cfg.GetDatabaseParams()
		ctx := cmd.Context()

		err := db.MigrateUp(dbParams)
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}

		dbPool := db.BuildDatabaseConnection(ctx, dbParams)
		defer dbPool.Close()
		if !migrate.CheckMigrationRequired(ctx, dbPool) {
			fmt.Println("Migration already completed")
			os.Exit(0)
		}

		migrateTool, err := migrate.NewMigrate(dbPool, ident.NewHexAddressProvider())
		if err != nil {
			fmt.Println("Failed to create a new migrate:", err)
			os.Exit(1)
		}

		migrateTool.SetReporter(&migrateReporter{})

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
}
