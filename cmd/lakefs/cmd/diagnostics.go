package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

var diagnosticsCmd = &cobra.Command{
	Use:   "diagnostics",
	Short: "Collect lakeFS diagnostics",
	Run: func(cmd *cobra.Command, args []string) {
		output, _ := cmd.Flags().GetString("output")
		log.Printf("Currently nothing is collected (%s)", output)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(diagnosticsCmd)
	diagnosticsCmd.Flags().StringP("output", "o", "lakefs-diagnostics.zip", "output zip filename")
}
