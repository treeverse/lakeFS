package cmd

import (
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
		ctx := cmd.Context()
		output, _ := cmd.Flags().GetString("output")

		dbPool := db.BuildDatabaseConnection(ctx, cfg.GetDatabaseParams())
		defer dbPool.Close()
		adapter, err := factory.BuildBlockAdapter(ctx, cfg)
		if err != nil {
			log.Printf("Failed to create block adapter: %s", err)
		}
		c, err := catalog.New(ctx, catalog.Config{
			Config: cfg,
			DB:     dbPool,
		})
		if err != nil {
			log.Printf("Failed to create c: %s", err)
		}
		pyrmaidParams, err := cfg.GetCommittedTierFSParams(ctx)
		if err != nil {
			log.Printf("Failed to get pyramid params: %s", err)
		}

		collector := diagnostics.NewCollector(dbPool, c, pyrmaidParams, adapter)

		f, err := os.Create(output)
		if err != nil {
			log.Fatalf("Create zip file '%s' failed - %s", output, err)
		}
		defer func() { _ = f.Close() }()

		log.Printf("Collecting data")
		err = collector.Collect(ctx, f)
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
