package cmd

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authUsersList = &cobra.Command{
	Use:   "list",
	Short: "List users",
	Run: func(cmd *cobra.Command, args []string) {
		amount := Must(cmd.Flags().GetInt("amount"))
		after := Must(cmd.Flags().GetString("after"))

		clt := getClient()

		resp, err := clt.ListUsersWithResponse(cmd.Context(), &apigen.ListUsersParams{
			After:  apiutil.Ptr(after),
			Amount: apiutil.Ptr(amount),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		users := resp.JSON200.Results
		rows := make([][]interface{}, len(users))
		for i, user := range users {
			ts := time.Unix(user.CreationDate, 0).String()
			rows[i] = []interface{}{user.Id, ts}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"User ID", "Creation Date"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	addPaginationFlags(authUsersList)

	authUsersCmd.AddCommand(authUsersList)
}
