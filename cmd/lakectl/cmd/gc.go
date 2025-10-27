package cmd

import (
	"github.com/spf13/cobra"
)

var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "Manage the garbage collection policy",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(gcCmd)
}
