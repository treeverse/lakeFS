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

// statementOut mirrors apigen.Statement for JSON marshaling, using json.RawMessage
// for Resource to allow rendering multi-resource array strings as proper JSON arrays.
type statementOut struct {
	Action    []string                    `json:"action"`
	Condition *apigen.Statement_Condition `json:"condition,omitempty"`
	Effect    string                      `json:"effect"`
	Resource  json.RawMessage             `json:"resource"`
}

func (s StatementDoc) MarshalJSON() ([]byte, error) {
	out := struct {
		Statement []statementOut `json:"statement"`
	}{Statement: make([]statementOut, len(s.Statement))}

	for i, st := range s.Statement {
		// Multi-resource policies store resources as a JSON-encoded array string.
		// Expand it into a proper JSON array for display.
		var arr []string
		var res json.RawMessage
		var err error
		if json.Unmarshal([]byte(st.Resource), &arr) == nil {
			res, err = json.Marshal(arr)
		} else {
			res, err = json.Marshal(st.Resource)
		}
		if err != nil {
			return nil, err
		}
		out.Statement[i] = statementOut{st.Action, st.Condition, st.Effect, res}
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
