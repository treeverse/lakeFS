package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authUsersPoliciesAttach = &cobra.Command{
	Use:   "attach",
	Short: "Attach a policy to a user",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		policy := Must(cmd.Flags().GetString("policy"))
		clt := getClient()
		resp, err := clt.AttachPolicyToUserWithResponse(cmd.Context(), id, policy)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		fmt.Println("Policy attached successfully")
	},
}

//nolint:gochecknoinits
func init() {
	authUsersPoliciesAttach.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersPoliciesAttach.MarkFlagRequired("id")
	authUsersPoliciesAttach.Flags().String("policy", "", "Policy identifier")
	_ = authUsersPoliciesAttach.MarkFlagRequired("policy")

	authUsersPolicies.AddCommand(authUsersPoliciesAttach)
}
