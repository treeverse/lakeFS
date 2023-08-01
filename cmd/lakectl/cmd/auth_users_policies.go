package cmd

import "github.com/spf13/cobra"

var authUsersPolicies = &cobra.Command{
	Use:   "policies",
	Short: "Manage user policies",
	Long:  "Manage user policies.  Requires an external authorization server with matching support.",
}

//nolint:gochecknoinits
func init() {
	authUsersCmd.AddCommand(authUsersPolicies)
}
