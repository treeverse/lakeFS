package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var branchListCmd = &cobra.Command{
	Use:               "list <repository URI>",
	Short:             "List branches in a repository",
	Example:           "lakectl branch list " + myRepoExample,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		prefix := Must(cmd.Flags().GetString("prefix"))
		after := Must(cmd.Flags().GetString("after"))
		amount := Must(cmd.Flags().GetInt("amount"))
		u := MustParseRepoURI("repository URI", args[0])
		client := getClient()
		resp, err := client.ListBranchesWithResponse(cmd.Context(), u.Repository, &apigen.ListBranchesParams{
			Prefix: apiutil.Ptr(apigen.PaginationPrefix(prefix)),
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
	branchListCmd.Flags().String("prefix", "", "filter according to this prefix")
	branchListCmd.Flags().String("after", "", "show results after this value (used for pagination)")
	branchListCmd.Flags().Int("amount", defaultAmountArgumentValue, "number of results to return")

	branchCmd.AddCommand(branchListCmd)
}
