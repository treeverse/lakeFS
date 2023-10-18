package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

var tagListCmd = &cobra.Command{
	Use:               "list <repository uri>",
	Short:             "List tags in a repository",
	Example:           "lakectl tag list lakefs://<repository>",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		amount := Must(cmd.Flags().GetInt("amount"))
		after := Must(cmd.Flags().GetString("after"))

		u := MustParseRepoURI("Operation requires a valid repository URI. e.g. lakefs://<repo>", args[0])

		ctx := cmd.Context()
		client := getClient()
		resp, err := client.ListTagsWithResponse(ctx, u.Repository, &apigen.ListTagsParams{
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

		tmplArgs := struct {
			TagTable   *Table
			Pagination *Pagination
		}{
			TagTable: &Table{
				Headers: []interface{}{"Tag", "Commit ID"},
				Rows:    rows,
			},
		}
		pagination := resp.JSON200.Pagination
		if pagination.HasMore {
			tmplArgs.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}
		PrintTable(rows, []interface{}{"Tag", "Commit ID"}, &pagination, amount)
	},
}

//nolint:gochecknoinits
func init() {
	flags := tagListCmd.Flags()
	flags.Int("amount", defaultAmountArgumentValue, "number of results to return")
	flags.String("after", "", "show results after this value (used for pagination)")

	tagCmd.AddCommand(tagListCmd)
}
