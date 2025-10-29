package cmd

import (
	"github.com/spf13/cobra"
)

var actionsRunsCmd = &cobra.Command{
	Use:   "runs",
	Short: "Explore runs information",
}

//nolint:gochecknoinits
func init() {
	actionsCmd.AddCommand(actionsRunsCmd)
}
