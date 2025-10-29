package cmd

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "List policies",
	Run: func(cmd *cobra.Command, args []string) {
		prefix, after, amount := getPaginationFlags(cmd)

		clt := getClient()

		resp, err := clt.ListPoliciesWithResponse(cmd.Context(), &apigen.ListPoliciesParams{
			Prefix: apiutil.Ptr(apigen.PaginationPrefix(prefix)),
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		policies := resp.JSON200.Results
		rows := make([][]interface{}, len(policies))
		for i, policy := range policies {
			ts := time.Unix(*policy.CreationDate, 0).String()
			rows[i] = []interface{}{policy.Id, ts}
		}
		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Policy ID", "Creation Date"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	withPaginationFlags(authPoliciesList)

	authPoliciesCmd.AddCommand(authPoliciesList)
}
