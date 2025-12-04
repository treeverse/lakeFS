package cmd

import (
	"github.com/spf13/cobra"
)

// metadataCmd represents the metadata command
var metadataCmd = &cobra.Command{
	Use:    "metadata",
	Short:  "Explore and analyze repository metadata",
	Hidden: true,
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(metadataCmd)
}
