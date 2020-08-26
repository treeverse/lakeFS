package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/treeverse/lakefs/db/params"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/db"
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

func connectToDB(connectionString string) db.Database {
	database, err := db.ConnectDB(params.Database{Driver: db.DatabaseDriver, ConnectionString: connectionString})
	if err != nil {
		fmt.Printf("Failed connecting to database: %s\n", err)
		os.Exit(1)
	}
	return database
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(dbCmd)
	dbCmd.PersistentFlags().String("db", "postgres://127.0.0.1:5432/postgres?sslmode=disable", "Database connection string")
	dbCmd.PersistentFlags().Int("requests", 1000, "Number of requests to performs")
	dbCmd.PersistentFlags().Float64("sample", 0.5, "Measure sample ratio (between 0 and 1)")
	dbCmd.PersistentFlags().Int("concurrency", 1, "Number of concurrent workers")
	dbCmd.PersistentFlags().String("repository", "", "Name of the repository (empty will create random one)")
}
