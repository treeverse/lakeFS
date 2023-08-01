package cmd

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

var repoListCmd = &cobra.Command{
	Use:   "list",
	Short: "List repositories",
	Args:  cobra.NoArgs,
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return nil, cobra.ShellCompDirectiveNoFileComp
	},
	Run: func(cmd *cobra.Command, args []string) {
		amount := Must(cmd.Flags().GetInt("amount"))
		after := Must(cmd.Flags().GetString("after"))
		clt := getClient()

		resp, err := clt.ListRepositoriesWithResponse(cmd.Context(), &api.ListRepositoriesParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		repos := resp.JSON200.Results
		rows := make([][]interface{}, len(repos))
		for i, repo := range repos {
			ts := time.Unix(repo.CreationDate, 0).String()
			rows[i] = []interface{}{repo.Id, ts, repo.DefaultBranch, repo.StorageNamespace}
		}
		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Repository", "Creation Date", "Default Ref Name", "Storage Namespace"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	repoListCmd.Flags().Int("amount", defaultAmountArgumentValue, "number of results to return")
	repoListCmd.Flags().String("after", "", "show results after this value (used for pagination)")

	repoCmd.AddCommand(repoListCmd)
}
