package cmd

import "github.com/spf13/cobra"

var authGroupsMembersCmd = &cobra.Command{
	Use:   "members",
	Short: "Manage group user memberships",
}

//nolint:gochecknoinits
func init() {
	authGroupsCmd.AddCommand(authGroupsMembersCmd)
}
