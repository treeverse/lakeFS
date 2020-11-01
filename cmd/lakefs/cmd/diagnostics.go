package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/treeverse/lakefs/diagnostics"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/db"
)

// diagnosticsCmd represents the diagnostics command
var diagnosticsCmd = &cobra.Command{
	Use:   "diagnostics",
	Short: "Collect lakeFS diagnostics",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		//conf := config.NewConfig()
		output, _ := cmd.Flags().GetString("output")

		dbPool := db.BuildDatabaseConnection(cfg.GetDatabaseParams())
		defer dbPool.Close()

		c := diagnostics.NewCollector(dbPool)
		f, err := os.Create(output)
		if err != nil {
			fmt.Printf("Failed to open file! %s\n", err)
		}
		defer func() { _ = f.Close() }()

		fmt.Println("Collecting...")
		err = c.Collect(ctx, f)
		if err != nil {
			fmt.Printf("Failed to collect data: %s\n", err)
		}

		fmt.Printf("Diagnostics collected into zip file: %s\n", output)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(diagnosticsCmd)
	diagnosticsCmd.Flags().StringP("output", "o", "", "output zip filename")
	_ = diagnosticsCmd.MarkFlagRequired("output")
}
