package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// bisectViewCmd represents the log command
var bisectViewCmd = &cobra.Command{
	Use:   "view",
	Short: "Current bisect commits",
	Run: func(cmd *cobra.Command, args []string) {
		var state Bisect
		err := state.Load()
		if os.IsNotExist(err) {
			Die(`You need to start by "bisect start"`, 1)
		}
		if len(state.Commits) == 0 {
			state.PrintStatus()
			return
		}
		Write(bisectCommitTemplate, state.Commits)
	},
}

//nolint:gochecknoinits
func init() {
	bisectCmd.AddCommand(bisectViewCmd)
}
