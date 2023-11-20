package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authGroupsMembersList = &cobra.Command{
	Use:   "list",
	Short: "List users in a group",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		amount := Must(cmd.Flags().GetInt("amount"))
		after := Must(cmd.Flags().GetString("after"))

		clt := getClient()

		resp, err := clt.ListGroupMembersWithResponse(cmd.Context(), id, &apigen.ListGroupMembersParams{
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
			rows[i] = []interface{}{user.Id}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"User ID"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsMembersList.Flags().String("id", "", "Group identifier")
	_ = authGroupsMembersList.MarkFlagRequired("id")
	addPaginationFlags(authGroupsMembersList)

	authGroupsMembersCmd.AddCommand(authGroupsMembersList)
}
