package cmd

import "github.com/spf13/cobra"

var authUsersCredentials = &cobra.Command{
	Use:   "credentials",
	Short: "Manage user credentials",
}

//nolint:gochecknoinits
func init() {
	authUsersCmd.AddCommand(authUsersCredentials)
}
