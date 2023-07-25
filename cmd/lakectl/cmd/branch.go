package cmd

import (
	"github.com/spf13/cobra"
)

// branchCmd represents the branch command
var branchCmd = &cobra.Command{
	Use:   "branch",
	Short: "Create and manage branches within a repository",
	Long:  `Create delete and list branches within a lakeFS repository`,
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(branchCmd)
}
