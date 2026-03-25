package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

type StatementDoc struct {
	Statement []apigen.Statement `json:"statement"`
}

const policyDetailsTemplate = `
ID: {{ .ID | bold }}
Creation Date: {{  .CreationDate | date }}
Statements:
{{ .StatementDoc | json }}

`

var authPoliciesShow = &cobra.Command{
	Use:   "show",
	Short: "Show a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		clt := getClient()

		resp, err := clt.GetPolicyWithResponse(cmd.Context(), id)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		policy := *resp.JSON200
		Write(policyDetailsTemplate, struct {
			ID           string
			CreationDate int64
			StatementDoc StatementDoc
		}{
			ID:           policy.Id,
			CreationDate: *policy.CreationDate,
			StatementDoc: StatementDoc{Statement: policy.Statement},
		})
	},
}

//nolint:gochecknoinits
func init() {
	authPoliciesShow.Flags().String("id", "", "Policy identifier")
	_ = authPoliciesShow.MarkFlagRequired("id")

	authPoliciesCmd.AddCommand(authPoliciesShow)
}
