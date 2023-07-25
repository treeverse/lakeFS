package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authUsersCredentialsDelete = &cobra.Command{
	Use:   "delete",
	Short: "Delete user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		accessKeyID, _ := cmd.Flags().GetString("access-key-id")
		clt := getClient()

		if id == "" {
			resp, err := clt.GetCurrentUserWithResponse(cmd.Context())
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			id = resp.JSON200.User.Id
		}
		resp, err := clt.DeleteCredentialsWithResponse(cmd.Context(), id, accessKeyID)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)

		fmt.Printf("Credentials deleted successfully\n")
	},
}

//nolint:gochecknoinits
func init() {
	authUsersCredentialsDelete.Flags().String("id", "", "Username (email for password-based users, default: current user)")
	authUsersCredentialsDelete.Flags().String("access-key-id", "", "Access key ID to delete")
	_ = authUsersCredentialsDelete.MarkFlagRequired("access-key-id")

	authUsersCredentials.AddCommand(authUsersCredentialsDelete)
}
