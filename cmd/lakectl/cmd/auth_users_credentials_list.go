package cmd

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authUsersCredentialsList = &cobra.Command{
	Use:   "list",
	Short: "List user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		amount := Must(cmd.Flags().GetInt("amount"))
		after := Must(cmd.Flags().GetString("after"))
		id := Must(cmd.Flags().GetString("id"))

		clt := getClient()
		if id == "" {
			resp, err := clt.GetCurrentUserWithResponse(cmd.Context())
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			id = resp.JSON200.User.Id
		}

		resp, err := clt.ListUserCredentialsWithResponse(cmd.Context(), id, &apigen.ListUserCredentialsParams{
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		credentials := resp.JSON200.Results
		rows := make([][]interface{}, len(credentials))
		for i, c := range credentials {
			ts := time.Unix(c.CreationDate, 0).String()
			rows[i] = []interface{}{c.AccessKeyId, ts}
		}
		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Access Key ID", "Issued Date"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	authUsersCredentialsList.Flags().String("id", "", "Username (email for password-based users, default: current user)")
	addPaginationFlags(authUsersCredentialsList)

	authUsersCredentials.AddCommand(authUsersCredentialsList)
}
