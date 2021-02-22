package cmd

import (
	"context"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/diagnostics"
)

// diagnosticsCmd represents the diagnostics command
var diagnosticsCmd = &cobra.Command{
	Use:   "diagnostics",
	Short: "Collect lakeFS diagnostics",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		output, _ := cmd.Flags().GetString("output")

		dbPool := db.BuildDatabaseConnection(cfg.GetDatabaseParams())
		defer dbPool.Close()
		adapter, err := factory.BuildBlockAdapter(cfg)
		if err != nil {
			log.Printf("Failed to create block adapter: %s", err)
		}
		cataloger, err := catalog.NewCataloger(catalog.Config{
			Config: cfg,
			DB:     dbPool,
		})
		if err != nil {
			log.Printf("Failed to create cataloger: %s", err)
		}
		pyrmaidParams, err := cfg.GetCommittedTierFSParams()
		if err != nil {
			log.Printf("Failed to get pyramid params: %s", err)
		}

		c := diagnostics.NewCollector(dbPool, cataloger, pyrmaidParams, adapter)

		f, err := os.Create(output)
		if err != nil {
			log.Fatalf("Create zip file '%s' failed - %s", output, err)
		}
		defer func() { _ = f.Close() }()

		log.Printf("Collecting data")
		err = c.Collect(ctx, f)
		if err != nil {
			log.Fatalf("Failed to collect data: %s", err)
		}
		log.Printf("Diagnostics collected into %s", output)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(diagnosticsCmd)
	diagnosticsCmd.Flags().StringP("output", "o", "lakefs-diagnostics.zip", "output zip filename")
}
