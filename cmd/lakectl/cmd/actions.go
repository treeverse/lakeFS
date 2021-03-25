package cmd

import (
	"github.com/spf13/cobra"
)

var actionsCmd = &cobra.Command{
	Use:   "actions",
	Short: "Manage Actions commands",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(actionsCmd)
}
