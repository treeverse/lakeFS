package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var authIAMExternalPrincipalsAdd = &cobra.Command{
	Use:   "add",
	Short: "Add an external principal (IAM role) to a user",
	Run: func(cmd *cobra.Command, args []string) {
		userID := Must(cmd.Flags().GetString("user-id"))
		principalID := Must(cmd.Flags().GetString("principal-id"))
		clt := getClient()

		resp, err := clt.CreateUserExternalPrincipalWithResponse(cmd.Context(), userID, &apigen.CreateUserExternalPrincipalParams{
			PrincipalId: principalID,
		}, apigen.CreateUserExternalPrincipalJSONRequestBody{})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		fmt.Println("External principal added successfully")
	},
}

//nolint:gochecknoinits
func init() {
	authIAMExternalPrincipalsAdd.Flags().String("user-id", "", "User ID to add the external principal to")
	_ = authIAMExternalPrincipalsAdd.MarkFlagRequired("user-id")
	authIAMExternalPrincipalsAdd.Flags().String("principal-id", "", "External principal ID (e.g., AWS IAM role ARN)")
	_ = authIAMExternalPrincipalsAdd.MarkFlagRequired("principal-id")

	authIAMExternalPrincipals.AddCommand(authIAMExternalPrincipalsAdd)
}
