package cmd

import (
	"github.com/spf13/cobra"
)

// fsCmd represents the fs command
var fsCmd = &cobra.Command{
	Use:   "fs",
	Short: "View and manipulate objects",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(fsCmd)
}
