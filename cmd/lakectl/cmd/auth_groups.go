package cmd

import "github.com/spf13/cobra"

// groups
var authGroupsCmd = &cobra.Command{
	Use:   "groups",
	Short: "Manage groups",
}

//nolint:gochecknoinits
func init() {
	authCmd.AddCommand(authGroupsCmd)
}
