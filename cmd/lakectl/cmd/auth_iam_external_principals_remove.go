package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var authIAMExternalPrincipalsRemove = &cobra.Command{
	Use:   "remove",
	Short: "Remove an external principal (IAM role) from a user",
	Run: func(cmd *cobra.Command, args []string) {
		userID := Must(cmd.Flags().GetString("user-id"))
		principalID := Must(cmd.Flags().GetString("principal-id"))
		clt := getClient()

		resp, err := clt.DeleteUserExternalPrincipalWithResponse(cmd.Context(), userID, &apigen.DeleteUserExternalPrincipalParams{
			PrincipalId: principalID,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Println("External principal removed successfully")
	},
}

//nolint:gochecknoinits
func init() {
	authIAMExternalPrincipalsRemove.Flags().String("user-id", "", "User ID to remove the external principal from")
	_ = authIAMExternalPrincipalsRemove.MarkFlagRequired("user-id")
	authIAMExternalPrincipalsRemove.Flags().String("principal-id", "", "External principal ID (e.g., AWS IAM role ARN)")
	_ = authIAMExternalPrincipalsRemove.MarkFlagRequired("principal-id")

	authIAMExternalPrincipals.AddCommand(authIAMExternalPrincipalsRemove)
}
