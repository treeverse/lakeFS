package cmd

import "github.com/spf13/cobra"

// policies
var authPoliciesCmd = &cobra.Command{
	Use:   "policies",
	Short: "Manage policies",
}

//nolint:gochecknoinits
func init() {
	authCmd.AddCommand(authPoliciesCmd)
}
