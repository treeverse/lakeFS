package cmd

import (
	"encoding/json"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

type StatementDoc struct {
	Statement []apigen.Statement `json:"statement"`
}

// printStatement mirrors apigen.Statement but uses json.RawMessage for Resource,
// so that a multi-resource value stored as a JSON array string is printed as an array.
type printStatement struct {
	Action    []string                    `json:"action"`
	Condition *apigen.Statement_Condition `json:"condition,omitempty"`
	Effect    string                      `json:"effect"`
	Resource  json.RawMessage             `json:"resource"`
}

func toPrintStatement(s apigen.Statement) printStatement {
	var arr []string
	if err := json.Unmarshal([]byte(s.Resource), &arr); err == nil {
		raw, _ := json.Marshal(arr)
		return printStatement{Action: s.Action, Condition: s.Condition, Effect: s.Effect, Resource: raw}
	}
	raw, _ := json.Marshal(s.Resource)
	return printStatement{Action: s.Action, Condition: s.Condition, Effect: s.Effect, Resource: raw}
}

type showStatementDoc struct {
	Statement []printStatement `json:"statement"`
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
		stmts := make([]printStatement, len(policy.Statement))
		for i, s := range policy.Statement {
			stmts[i] = toPrintStatement(s)
		}
		Write(policyDetailsTemplate, struct {
			ID           string
			CreationDate int64
			StatementDoc showStatementDoc
		}{
			ID:           policy.Id,
			CreationDate: *policy.CreationDate,
			StatementDoc: showStatementDoc{Statement: stmts},
		})
	},
}

//nolint:gochecknoinits
func init() {
	authPoliciesShow.Flags().String("id", "", "Policy identifier")
	_ = authPoliciesShow.MarkFlagRequired("id")

	authPoliciesCmd.AddCommand(authPoliciesShow)
}
