package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authGroupsMembersRemove = &cobra.Command{
	Use:   "remove",
	Short: "Remove a user from a group",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		user := Must(cmd.Flags().GetString("user"))
		clt := getClient()

		resp, err := clt.DeleteGroupMembershipWithResponse(cmd.Context(), id, user)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Println("User successfully removed")
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsMembersRemove.Flags().String("id", "", "Group identifier")
	authGroupsMembersRemove.Flags().String("user", "", "Username (email for password-based users, default: current user)")
	_ = authGroupsMembersRemove.MarkFlagRequired("id")
	_ = authGroupsMembersRemove.MarkFlagRequired("user")

	authGroupsMembersCmd.AddCommand(authGroupsMembersRemove)
}
