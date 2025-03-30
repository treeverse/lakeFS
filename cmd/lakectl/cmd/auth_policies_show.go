package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/auth"
)

type MultipleResourceStatement struct {
	Action   []string `json:"action"`
	Effect   string   `json:"effect"`
	Resource []string `json:"resource"`
}

type MultipleResourceStatementDoc struct {
	Statement []MultipleResourceStatement `json:"statement"`
}
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
		resourceArray := Must(cmd.Flags().GetBool("resource-array"))
		clt := getClient()

		resp, err := clt.GetPolicyWithResponse(cmd.Context(), id)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		policy := *resp.JSON200
		if resourceArray {
			var statementsV2 []MultipleResourceStatement
			for _, s := range policy.Statement {
				resources, _ := auth.ParsePolicyResourceAsList(s.Resource)
				newStatement := MultipleResourceStatement{
					Action:   s.Action,
					Effect:   s.Effect,
					Resource: resources}
				statementsV2 = append(statementsV2, newStatement)
			}
			Write(policyDetailsTemplate, struct {
				ID           string
				CreationDate int64
				StatementDoc MultipleResourceStatementDoc
			}{
				ID:           policy.Id,
				CreationDate: *policy.CreationDate,
				StatementDoc: MultipleResourceStatementDoc{Statement: statementsV2},
			})
			return
		}
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
	authPoliciesShow.Flags().Bool("resource-array", false, "Display resources as an array in the output")
	authPoliciesShow.Flags().String("id", "", "Policy identifier")
	_ = authPoliciesShow.MarkFlagRequired("id")

	authPoliciesCmd.AddCommand(authPoliciesShow)
}
