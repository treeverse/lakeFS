package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var authUsersAWSIAMAttach = &cobra.Command{
	Use:   "attach",
	Short: "Attach an external principal (IAM role) to a user",
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

		resp, err := clt.CreateUserExternalPrincipalWithResponse(cmd.Context(), id, &apigen.CreateUserExternalPrincipalParams{
			PrincipalId: principalID,
		}, apigen.CreateUserExternalPrincipalJSONRequestBody{})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		fmt.Println("External principal attached successfully")
	},
}

//nolint:gochecknoinits
func init() {
	authUsersAWSIAMAttach.Flags().String("id", "", "Username (email for password-based users, default: current user)")
	authUsersAWSIAMAttach.Flags().String("principal-id", "", "External principal ID (e.g., AWS IAM role ARN)")
	_ = authUsersAWSIAMAttach.MarkFlagRequired("principal-id")

	authUsersAWSIAMCmd.AddCommand(authUsersAWSIAMAttach)
}
