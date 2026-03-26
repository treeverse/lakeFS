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

func (s StatementDoc) MarshalJSON() ([]byte, error) {
	type stmtOut struct {
		Action    []string                    `json:"action"`
		Condition *apigen.Statement_Condition `json:"condition,omitempty"`
		Effect    string                      `json:"effect"`
		Resource  json.RawMessage             `json:"resource"`
	}
	out := struct {
		Statement []stmtOut `json:"statement"`
	}{Statement: make([]stmtOut, len(s.Statement))}

	for i, st := range s.Statement {
		// Multi-resource policies store resources as a JSON-encoded array string.
		// Expand it into a proper JSON array for display.
		var arr []string
		var res json.RawMessage
		if json.Unmarshal([]byte(st.Resource), &arr) == nil {
			res, _ = json.Marshal(arr)
		} else {
			res, _ = json.Marshal(st.Resource)
		}
		out.Statement[i] = stmtOut{st.Action, st.Condition, st.Effect, res}
	}
	return json.Marshal(out)
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
