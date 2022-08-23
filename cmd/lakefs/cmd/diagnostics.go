package cmd

import (
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/diagnostics"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/stats"
)

// diagnosticsCmd represents the diagnostics command
var diagnosticsCmd = &cobra.Command{
	Use:   "diagnostics",
	Short: "Collect lakeFS diagnostics",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		ctx := cmd.Context()
		output, _ := cmd.Flags().GetString("output")

		dbParams := cfg.GetDatabaseParams()
		var (
			dbPool       db.Database
			storeMessage *kv.StoreMessage
		)
		if dbParams.KVEnabled {
			kvParams := cfg.GetKVParams()
			kvStore, err := kv.Open(ctx, kvParams)
			if err != nil {
				log.Fatalf("Failed to open KV store: %s", err)
			}
			defer kvStore.Close()
			storeMessage = &kv.StoreMessage{
				Store: kvStore,
			}
		} else {
			dbPool = db.BuildDatabaseConnection(ctx, cfg.GetDatabaseParams())
			defer dbPool.Close()
		}

		adapter, err := factory.BuildBlockAdapter(ctx, &stats.NullCollector{}, cfg)
		if err != nil {
			log.Printf("Failed to create block adapter: %s", err)
		}
		c, err := catalog.New(ctx, catalog.Config{
			Config:  cfg,
			DB:      dbPool,
			KVStore: storeMessage,
		})
		if err != nil {
			log.Fatalf("Failed to create catalog: %s", err)
		}
		defer func() { _ = c.Close() }()
		pyramidParams, err := cfg.GetCommittedTierFSParams(adapter)
		if err != nil {
			log.Fatalf("Failed to get pyramid params: %s", err)
		}

		collector := diagnostics.NewCollector(c, pyramidParams, adapter)

		f, err := os.Create(output)
		if err != nil {
			log.Fatalf("Create zip file '%s' failed: %s", output, err)
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
