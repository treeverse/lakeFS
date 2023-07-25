package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authGroupsAddMember = &cobra.Command{
	Use:   "add",
	Short: "Add a user to a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		user, _ := cmd.Flags().GetString("user")
		clt := getClient()

		resp, err := clt.AddGroupMembershipWithResponse(cmd.Context(), id, user)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		fmt.Printf("User successfully added\n")
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsAddMember.Flags().String("id", "", "Group identifier")
	authGroupsAddMember.Flags().String("user", "", "Username (email for password-based users, default: current user)")
	_ = authGroupsAddMember.MarkFlagRequired("id")
	_ = authGroupsAddMember.MarkFlagRequired("user")

	authGroupsMembersCmd.AddCommand(authGroupsAddMember)
}
