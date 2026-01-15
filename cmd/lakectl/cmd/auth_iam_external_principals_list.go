package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authIAMExternalPrincipalsList = &cobra.Command{
	Use:   "list",
	Short: "List external principals (IAM roles) attached to a user",
	Run: func(cmd *cobra.Command, args []string) {
		userID := Must(cmd.Flags().GetString("user-id"))
		prefix, after, amount := getPaginationFlags(cmd)
		clt := getClient()

		resp, err := clt.ListUserExternalPrincipalsWithResponse(cmd.Context(), userID, &apigen.ListUserExternalPrincipalsParams{
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
	authIAMExternalPrincipalsList.Flags().String("user-id", "", "User ID to list external principals for")
	_ = authIAMExternalPrincipalsList.MarkFlagRequired("user-id")
	withPaginationFlags(authIAMExternalPrincipalsList)

	authIAMExternalPrincipals.AddCommand(authIAMExternalPrincipalsList)
}
