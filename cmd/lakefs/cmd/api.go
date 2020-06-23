package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/logging"
)

// apiCmd represents the api command
var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "Run a lakeFS API Service",
	Run: func(cmd *cobra.Command, args []string) {
		logging.Default().WithField("version", config.Version).Info("lakeFS run API Service")
		runLakeFSServices(LakeFSServiceAPI)
	},
}

func init() {
	runCmd.AddCommand(apiCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// apiCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// apiCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
