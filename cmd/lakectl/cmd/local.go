package cmd

import (
	"github.com/spf13/cobra"
)

var localCmd = &cobra.Command{
	Use:   "local",
	Short: "sync local directories with remote lakeFS locations",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(localCmd)
}
