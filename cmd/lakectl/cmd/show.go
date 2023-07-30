package cmd

import (
	"github.com/spf13/cobra"
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show",
	Short: "See detailed information about an entity",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(showCmd)
}
