package cmd

import (
	"github.com/spf13/cobra"
)

// repoCmd represents the repo command
var repoCmd = &cobra.Command{
	Use:   "repo",
	Short: "Manage and explore repos",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(repoCmd)
}
