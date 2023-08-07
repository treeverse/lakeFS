package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// bisectLogCmd represents the log command
var bisectLogCmd = &cobra.Command{
	Use:   "log",
	Short: "Print out the current bisect state",
	Run: func(cmd *cobra.Command, args []string) {
		var state Bisect
		err := state.Load()
		if os.IsNotExist(err) {
			Die(`You need to start by "bisect start"`, 1)
		}
		state.PrintStatus()
	},
}

//nolint:gochecknoinits
func init() {
	bisectCmd.AddCommand(bisectLogCmd)
}
