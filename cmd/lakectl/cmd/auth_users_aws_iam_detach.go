package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var authUsersAWSIAMDetach = &cobra.Command{
	Use:   "detach",
	Short: "Detach an IAM Role from a lakeFS user",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		principalID := Must(cmd.Flags().GetString("principal-id"))
		clt := getClient()

		if id == "" {
			resp, err := clt.GetCurrentUserWithResponse(cmd.Context())
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			id = resp.JSON200.User.Id
		}

		resp, err := clt.DeleteUserExternalPrincipalWithResponse(cmd.Context(), id, &apigen.DeleteUserExternalPrincipalParams{
			PrincipalId: principalID,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Println("External principal detached successfully")
	},
}

//nolint:gochecknoinits
func init() {
	authUsersAWSIAMDetach.Flags().String("id", "", "Username (email for password-based users, default: current user)")
	authUsersAWSIAMDetach.Flags().String("principal-id", "", "External principal ID (e.g., AWS IAM role ARN)")
	_ = authUsersAWSIAMDetach.MarkFlagRequired("principal-id")

	authUsersAWSIAMCmd.AddCommand(authUsersAWSIAMDetach)
}
