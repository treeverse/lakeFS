package cmd

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authUsersGroupsList = &cobra.Command{
	Use:   "list",
	Short: "List groups for the given user",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		prefix, after, amount := getPaginationFlags(cmd)

		clt := getClient()

		resp, err := clt.ListUserGroupsWithResponse(cmd.Context(), id, &apigen.ListUserGroupsParams{
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
	authUsersGroupsList.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersGroupsList.MarkFlagRequired("id")
	withPaginationFlags(authUsersGroupsList)

	authUsersGroups.AddCommand(authUsersGroupsList)
}
