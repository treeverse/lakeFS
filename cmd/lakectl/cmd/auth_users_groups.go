package cmd

import "github.com/spf13/cobra"

var authUsersGroups = &cobra.Command{
	Use:   "groups",
	Short: "Manage user groups",
}

//nolint:gochecknoinits
func init() {
	authUsersCmd.AddCommand(authUsersGroups)
}
