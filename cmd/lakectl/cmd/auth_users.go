package cmd

import "github.com/spf13/cobra"

var authUsersCmd = &cobra.Command{
	Use:   "users",
	Short: "Manage users",
}

//nolint:gochecknoinits
func init() {
	authCmd.AddCommand(authUsersCmd)
}
