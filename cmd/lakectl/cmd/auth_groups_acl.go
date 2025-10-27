package cmd

import "github.com/spf13/cobra"

var authGroupsACLCmd = &cobra.Command{
	Use:   "acl [sub-command]",
	Short: "Manage ACLs",
	Long:  "manage ACLs of groups",
}

//nolint:gochecknoinits
func init() {
	authGroupsCmd.AddCommand(authGroupsACLCmd)
}
