package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authGroupsPoliciesAttach = &cobra.Command{
	Use:   "attach",
	Short: "Attach a policy to a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		policy, _ := cmd.Flags().GetString("policy")
		clt := getClient()

		resp, err := clt.AttachPolicyToGroupWithResponse(cmd.Context(), id, policy)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)

		fmt.Printf("Policy attached successfully\n")
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsPoliciesAttach.Flags().String("id", "", "User identifier")
	_ = authGroupsPoliciesAttach.MarkFlagRequired("id")
	authGroupsPoliciesAttach.Flags().String("policy", "", "Policy identifier")
	_ = authGroupsPoliciesAttach.MarkFlagRequired("policy")

	authGroupsPoliciesCmd.AddCommand(authGroupsPoliciesAttach)
}
