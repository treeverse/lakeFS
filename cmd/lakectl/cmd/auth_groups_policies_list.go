package cmd

import (
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authGroupsPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "List policies for the given group",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		prefix, after, amount := getPaginationFlags(cmd)

		clt := getClient()

		resp, err := clt.ListGroupPoliciesWithResponse(cmd.Context(), id, &apigen.ListGroupPoliciesParams{
			Prefix: apiutil.Ptr(apigen.PaginationPrefix(prefix)),
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		policies := resp.JSON200.Results
		rows := make([][]interface{}, 0)
		for _, policy := range policies {
			for i, statement := range policy.Statement {
				ts := time.Unix(*policy.CreationDate, 0).String()
				rows = append(rows, []interface{}{policy.Id, ts, i, statement.Resource, statement.Effect, strings.Join(statement.Action, ", ")})
			}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Policy ID", "Creation Date", "Statement #", "Resource", "Effect", "Actions"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsPoliciesList.Flags().String("id", "", "Group identifier")
	_ = authGroupsPoliciesList.MarkFlagRequired("id")
	withPaginationFlags(authGroupsPoliciesList)

	authGroupsPoliciesCmd.AddCommand(authGroupsPoliciesList)
}
