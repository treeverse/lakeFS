package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/logging"
)

// s3gatewayCmd represents the s3gateway command
var s3gatewayCmd = &cobra.Command{
	Use:   "s3gateway",
	Short: "Run lakeFS S3 Gateway",
	Run: func(cmd *cobra.Command, args []string) {
		logging.Default().WithField("version", config.Version).Info("lakeFS run S3 Gateway")
		runLakeFSServices(LakeFSServiceS3Gateway)
	},
}

func init() {
	runCmd.AddCommand(s3gatewayCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// s3gatewayCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// s3gatewayCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
