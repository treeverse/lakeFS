package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authPoliciesDelete = &cobra.Command{
	Use:   "delete",
	Short: "Delete a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		clt := getClient()

		resp, err := clt.DeletePolicyWithResponse(cmd.Context(), id)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Println("Policy deleted successfully")
	},
}

//nolint:gochecknoinits
func init() {
	authPoliciesDelete.Flags().String("id", "", "Policy identifier")
	_ = authPoliciesDelete.MarkFlagRequired("id")

	authPoliciesCmd.AddCommand(authPoliciesDelete)
}
