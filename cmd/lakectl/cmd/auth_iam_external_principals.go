package cmd

import "github.com/spf13/cobra"

var authIAMExternalPrincipals = &cobra.Command{
	Use:   "external-principals",
	Short: "Manage external principals (IAM roles)",
}

//nolint:gochecknoinits
func init() {
	authIAMCmd.AddCommand(authIAMExternalPrincipals)
}
