package cmd

import (
	"github.com/spf13/cobra"
)

// tagCmd represents the tag command
var tagCmd = &cobra.Command{
	Use:   "tag",
	Short: "Create and manage tags within a repository",
	Long:  `Create delete and list tags within a lakeFS repository`,
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(tagCmd)
}
