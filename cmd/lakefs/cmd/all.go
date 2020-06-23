package cmd

import (
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/logging"

	"github.com/spf13/cobra"
)

// allCmd represents the all command
var allCmd = &cobra.Command{
	Use:   "all",
	Short: "Run a lakeFS services",
	Run: func(cmd *cobra.Command, args []string) {
		logging.Default().WithField("version", config.Version).Info("lakeFS run all")
		runLakeFSServices(LakeFSServiceAPI | LakeFSServiceS3Gateway)
	},
}

func init() {
	runCmd.AddCommand(allCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// allCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// allCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
