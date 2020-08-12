package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/nessie"
	"os"
)

// rootCmd represents the base command when called without any sub-commands
var rootCmd = &cobra.Command{
	Use:   "lakefs-nessie",
	Short: "Run a system test on a lakeFS instance.",
	Run: func(cmd *cobra.Command, args []string) {
		if err := nessie.Run(context.Background(), config); err != nil {
			panic(fmt.Errorf("nessie run failed: %w", err))
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var config = nessie.Config{}

//nolint:gochecknoinits
func init() {
	rootCmd.PersistentFlags().StringVarP(&config.BaseURL, "endpoint-url", "e", "localhost:8000", "URL endpoint of the lakeFS instance")
	rootCmd.PersistentFlags().StringVarP(&config.BucketPath, "bucket", "b", "s3://nessie-system-testing", "Bucket's path")
}
