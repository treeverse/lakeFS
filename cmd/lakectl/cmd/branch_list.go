package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var branchListCmd = &cobra.Command{
	Use:               "list <repository uri>",
	Short:             "List branches in a repository",
	Example:           "lakectl branch list lakefs://<repository>",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		amount := Must(cmd.Flags().GetInt("amount"))
		after := Must(cmd.Flags().GetString("after"))
		u := MustParseRepoURI("Operation requires a valid repository URI. e.g. lakefs://<repo>", args[0])
		client := getClient()
		resp, err := client.ListBranchesWithResponse(cmd.Context(), u.Repository, &apigen.ListBranchesParams{
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(amount)),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		refs := resp.JSON200.Results
		rows := make([][]interface{}, len(refs))
		for i, row := range refs {
			rows[i] = []interface{}{row.Id, row.CommitId}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Branch", "Commit ID"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	branchListCmd.Flags().Int("amount", defaultAmountArgumentValue, "number of results to return")
	branchListCmd.Flags().String("after", "", "show results after this value (used for pagination)")

	branchCmd.AddCommand(branchListCmd)
}
