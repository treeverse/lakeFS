package cmd

import (
	"time"

	"github.com/spf13/cobra"
)

type ReqResult struct {
	Err  error
	Took time.Duration
}

// dbCmd represents the db command
var dbCmd = &cobra.Command{
	Use:   "db",
	Short: "Load test database actions",
}

//nolint:gochecknoinits,gomnd
func init() {
	rootCmd.AddCommand(dbCmd)
	dbCmd.PersistentFlags().String("db", "postgres://127.0.0.1:5432/postgres?sslmode=disable", "Database connection string")
	dbCmd.PersistentFlags().Int("requests", 1000, "Number of requests to performs")
	dbCmd.PersistentFlags().Float64("sample", 0.5, "Measure sample ratio (between 0 and 1)")
	dbCmd.PersistentFlags().Int("concurrency", 1, "Number of concurrent workers")
}
