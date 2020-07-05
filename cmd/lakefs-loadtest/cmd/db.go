package cmd

import (
	"github.com/spf13/cobra"
)

// dbCmd represents the db command
var dbCmd = &cobra.Command{
	Use:   "db",
	Short: "Load test database actions",
}

func init() {
	rootCmd.AddCommand(dbCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dbCmd.PersistentFlags().String("foo", "", "A help for foo")
	dbCmd.PersistentFlags().String("db", "postgres://localhost:5432/postgres?sslmode=disable&search_path=lakefs_catalog", "Database connection string")
	dbCmd.PersistentFlags().IntP("requests", "r", 100, "Number of requests to performs")
	dbCmd.PersistentFlags().Float64("sample", 0.5, "Measure sample ratio (between 0 and 1)")
	dbCmd.PersistentFlags().IntP("concurrency", "c", 1, "Number of concurrent workers")
	dbCmd.PersistentFlags().String("repository", "", "Name of the repository (empty will create random one)")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dbCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
