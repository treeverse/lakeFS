package cmd

import (
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var authUsersPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "List policies for the given user",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		amount := Must(cmd.Flags().GetInt("amount"))
		after := Must(cmd.Flags().GetString("after"))
		effective := Must(cmd.Flags().GetBool("effective"))

		clt := getClient()

		resp, err := clt.ListUserPoliciesWithResponse(cmd.Context(), id, &apigen.ListUserPoliciesParams{
			After:     apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount:    apiutil.Ptr(apigen.PaginationAmount(amount)),
			Effective: &effective,
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
	authUsersPoliciesList.Flags().Bool("effective", false,
		"List all distinct policies attached to the user, including by group memberships")
	authUsersPoliciesList.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersPoliciesList.MarkFlagRequired("id")
	withPaginationFlags(authUsersPoliciesList)

	authUsersPolicies.AddCommand(authUsersPoliciesList)
}
