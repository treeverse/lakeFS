package cmd

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authGroupsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List groups",
	Run: func(cmd *cobra.Command, args []string) {
		prefix, after, amount := getPaginationFlags(cmd)
		clt := getClient()

		resp, err := clt.ListGroupsWithResponse(cmd.Context(), &apigen.ListGroupsParams{
			Prefix: apiutil.Ptr(apigen.PaginationPrefix(prefix)),
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		groups := resp.JSON200.Results
		rows := make([][]interface{}, len(groups))
		for i, group := range groups {
			ts := time.Unix(group.CreationDate, 0).String()
			rows[i] = []interface{}{group.Id, ts}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Group ID", "Creation Date"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	withPaginationFlags(authGroupsListCmd)

	authGroupsCmd.AddCommand(authGroupsListCmd)
}
