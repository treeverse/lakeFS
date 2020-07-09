package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/config"
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
	database, err := db.ConnectDB(config.DefaultDatabaseDriver, connectionString)
	if err != nil {
		fmt.Printf("Failed connecting to database: %s\n", err)
		os.Exit(1)
	}
	return database
}

func init() {
	rootCmd.AddCommand(dbCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dbCmd.PersistentFlags().String("foo", "", "A help for foo")
	dbCmd.PersistentFlags().String("db", "postgres://127.0.0.1:5432/postgres?sslmode=disable", "Database connection string")
	dbCmd.PersistentFlags().Int("requests", 1000, "Number of requests to performs")
	dbCmd.PersistentFlags().Float64("sample", 0.5, "Measure sample ratio (between 0 and 1)")
	dbCmd.PersistentFlags().Int("concurrency", 1, "Number of concurrent workers")
	dbCmd.PersistentFlags().String("repository", "", "Name of the repository (empty will create random one)")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dbCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
