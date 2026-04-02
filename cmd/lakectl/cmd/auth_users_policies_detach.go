package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authUsersPoliciesDetach = &cobra.Command{
	Use:   "detach",
	Short: "Detach a policy from a user",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		policy := Must(cmd.Flags().GetString("policy"))
		clt := getClient()

		resp, err := clt.DetachPolicyFromUserWithResponse(cmd.Context(), id, policy)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)

		fmt.Println("Policy detached successfully")
	},
}

//nolint:gochecknoinits
func init() {
	authUsersPoliciesDetach.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersPoliciesDetach.MarkFlagRequired("id")
	authUsersPoliciesDetach.Flags().String("policy", "", "Policy identifier")
	_ = authUsersPoliciesDetach.MarkFlagRequired("policy")

	authUsersPolicies.AddCommand(authUsersPoliciesDetach)
}
