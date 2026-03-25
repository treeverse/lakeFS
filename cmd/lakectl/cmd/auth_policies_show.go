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
	// Use a raw map so new fields added to apigen.Statement are picked up automatically.
	type rawStatement = map[string]json.RawMessage

	stmts := make([]rawStatement, len(s.Statement))
	for i, st := range s.Statement {
		b, err := json.Marshal(st)
		if err != nil {
			return nil, err
		}
		var m rawStatement
		_ = json.Unmarshal(b, &m)
		// Multi-resource policies store resources as a JSON-encoded array string.
		// Expand it into a proper JSON array for display.
		var resourceStr string
		var arr []string
		if json.Unmarshal(m["resource"], &resourceStr) == nil {
			if json.Unmarshal([]byte(resourceStr), &arr) == nil {
				m["resource"], _ = json.Marshal(arr)
			}
		}
		stmts[i] = m
	}
	return json.Marshal(struct {
		Statement []rawStatement `json:"statement"`
	}{Statement: stmts})
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
