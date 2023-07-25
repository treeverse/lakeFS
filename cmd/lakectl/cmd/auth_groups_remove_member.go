package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authGroupsRemoveMember = &cobra.Command{
	Use:   "remove",
	Short: "Remove a user from a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		user, _ := cmd.Flags().GetString("user")
		clt := getClient()

		resp, err := clt.DeleteGroupMembershipWithResponse(cmd.Context(), id, user)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Printf("User successfully removed\n")
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsRemoveMember.Flags().String("id", "", "Group identifier")
	authGroupsRemoveMember.Flags().String("user", "", "Username (email for password-based users, default: current user)")
	_ = authGroupsRemoveMember.MarkFlagRequired("id")
	_ = authGroupsRemoveMember.MarkFlagRequired("user")

	authGroupsMembersCmd.AddCommand(authGroupsRemoveMember)
}
