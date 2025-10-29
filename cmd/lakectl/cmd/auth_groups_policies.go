package cmd

import "github.com/spf13/cobra"

var authGroupsPoliciesCmd = &cobra.Command{
	Use:   "policies",
	Short: "Manage group policies",
	Long:  "Manage group policies.  Requires an external authorization server with matching support.",
}

//nolint:gochecknoinits
func init() {
	authGroupsCmd.AddCommand(authGroupsPoliciesCmd)
}
