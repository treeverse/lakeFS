package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authUsersAWSIAMList = &cobra.Command{
	Use:   "list",
	Short: "List all IAM roles attached to a lakeFS user",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		prefix, after, amount := getPaginationFlags(cmd)
		clt := getClient()

		if id == "" {
			resp, err := clt.GetCurrentUserWithResponse(cmd.Context())
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			id = resp.JSON200.User.Id
		}

		resp, err := clt.ListUserExternalPrincipalsWithResponse(cmd.Context(), id, &apigen.ListUserExternalPrincipalsParams{
			Prefix: apiutil.Ptr(apigen.PaginationPrefix(prefix)),
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		principals := resp.JSON200.Results
		rows := make([][]any, 0)
		for _, principal := range principals {
			rows = append(rows, []any{principal.Id, principal.UserId})
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []any{"Principal ID", "User ID"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	authUsersAWSIAMList.Flags().String("id", "", "User ID to list external principals for")
	withPaginationFlags(authUsersAWSIAMList)

	authUsersAWSIAMCmd.AddCommand(authUsersAWSIAMList)
}
