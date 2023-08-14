package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authGroupsPoliciesDetach = &cobra.Command{
	Use:   "detach",
	Short: "Detach a policy from a group",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		policy := Must(cmd.Flags().GetString("policy"))
		clt := getClient()

		resp, err := clt.DetachPolicyFromGroupWithResponse(cmd.Context(), id, policy)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)

		fmt.Println("Policy detached successfully")
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsPoliciesDetach.Flags().String("id", "", "User identifier")
	_ = authGroupsPoliciesDetach.MarkFlagRequired("id")
	authGroupsPoliciesDetach.Flags().String("policy", "", "Policy identifier")
	_ = authGroupsPoliciesDetach.MarkFlagRequired("policy")

	authGroupsPoliciesCmd.AddCommand(authGroupsPoliciesDetach)
}
