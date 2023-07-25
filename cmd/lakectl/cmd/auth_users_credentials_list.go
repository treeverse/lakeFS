package cmd

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

var authUsersCredentialsList = &cobra.Command{
	Use:   "list",
	Short: "List user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		id, _ := cmd.Flags().GetString("id")

		clt := getClient()
		if id == "" {
			resp, err := clt.GetCurrentUserWithResponse(cmd.Context())
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			id = resp.JSON200.User.Id
		}

		resp, err := clt.ListUserCredentialsWithResponse(cmd.Context(), id, &api.ListUserCredentialsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
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
	addPaginationFlags(authUsersCredentialsList)

	authUsersCredentialsList.Flags().String("id", "", "Username (email for password-based users, default: current user)")

	authUsersCredentials.AddCommand(authUsersCredentialsList)
}
