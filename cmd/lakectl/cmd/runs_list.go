package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/uri"
)

const actionsRunsListTemplate = `{{.ActionsRunsTable | table -}}
{{.Pagination | paginate }}
`

var runsListCmd = &cobra.Command{
	Use:     "list",
	Short:   "List runs",
	Long:    `List all runs on a repository optional filter by branch or commit`,
	Example: "lakectl actions runs list lakefs://<repository> [--branch <branch>] [--commit <commit_id>]",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		commit, _ := cmd.Flags().GetString("commit")
		branch, _ := cmd.Flags().GetString("branch")
		var pagination api.Pagination
		rows := make([][]interface{}, 0)

		u := uri.Must(uri.Parse(args[0]))
		if commit != "" && branch != "" {
			Die("Can't specify 'commit' and 'branch'", 1)
		}

		client := getClient()
		ctx := cmd.Context()

		// list runs with optional branch filter
		var optionalBranch *string
		if branch != "" {
			optionalBranch = &branch
		}
		var optionalCommit *string
		if commit != "" {
			optionalCommit = &commit
		}
		for {
			amountForPagination := amount
			if amountForPagination == -1 {
				amountForPagination = defaultPaginationAmount
			}
			resp, err := client.ListRepositoryRunsWithResponse(ctx, u.Repository, &api.ListRepositoryRunsParams{
				After:  api.PaginationAfterPtr(after),
				Amount: api.PaginationAmountPtr(amountForPagination),
				Branch: optionalBranch,
				Commit: optionalCommit,
			})
			DieOnResponseError(resp, err)

			results := resp.JSON200.Results
			for _, row := range results {
				rows = append(rows, []interface{}{
					row.RunId,
					row.EventType,
					row.StartTime,
					row.EndTime,
					row.Branch,
					row.CommitId,
					row.Status,
				})
			}
			pagination = resp.JSON200.Pagination
			if amount != -1 || !pagination.HasMore {
				break
			}
			after = pagination.NextOffset
		}
		data := struct {
			ActionsRunsTable *Table
			Pagination       *Pagination
		}{
			ActionsRunsTable: &Table{
				Headers: []interface{}{
					"Run ID",
					"Event",
					"Start Time",
					"End Time",
					"Branch",
					"Commit ID",
					"Status",
				},
				Rows: rows,
			},
		}
		if pagination.HasMore {
			data.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}

		Write(actionsRunsListTemplate, data)
	},
}

//nolint:gochecknoinits
func init() {
	actionsRunsCmd.AddCommand(runsListCmd)
	runsListCmd.Flags().Int("amount", -1, "number of results to return. By default, all results are returned.")
	runsListCmd.Flags().String("after", "", "show results after this value (used for pagination)")
	runsListCmd.Flags().String("branch", "", "show results for specific branch")
	runsListCmd.Flags().String("commit", "", "show results for specific commit ID")
}
