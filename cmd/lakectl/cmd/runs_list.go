package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/api"
)

const actionsRunsListTemplate = `{{.ActionsRunsTable | table -}}
{{.Pagination | paginate }}
`

var runsListCmd = &cobra.Command{
	Use:               "list",
	Short:             "List runs",
	Long:              `List all runs on a repository optional filter by branch or commit`,
	Example:           "lakectl actions runs list lakefs://<repository> [--branch <branch>] [--commit <commit_id>]",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		amount := utils.MustInt(cmd.Flags().GetInt("amount"))
		after := utils.MustString(cmd.Flags().GetString("after"))
		commit := utils.MustString(cmd.Flags().GetString("commit"))
		branch := utils.MustString(cmd.Flags().GetString("branch"))
		u := utils.MustParseRepoURI("repository", args[0])
		if commit != "" && branch != "" {
			utils.Die("Can't specify 'commit' and 'branch'", 1)
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

		resp, err := client.ListRepositoryRunsWithResponse(ctx, u.Repository, &api.ListRepositoryRunsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
			Branch: optionalBranch,
			Commit: optionalCommit,
		})
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			utils.Die("Bad response from server", 1)
		}

		results := resp.JSON200.Results
		rows := make([][]interface{}, len(results))
		for i, row := range results {
			rows[i] = []interface{}{
				row.RunId,
				row.EventType,
				row.StartTime,
				row.EndTime,
				row.Branch,
				row.CommitId,
				row.Status,
			}
		}

		pagination := resp.JSON200.Pagination
		data := struct {
			ActionsRunsTable *utils.Table
			Pagination       *utils.Pagination
		}{
			ActionsRunsTable: &utils.Table{
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
			data.Pagination = &utils.Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}

		utils.Write(actionsRunsListTemplate, data)
	},
}

//nolint:gochecknoinits
func init() {
	actionsRunsCmd.AddCommand(runsListCmd)
	runsListCmd.Flags().Int("amount", utils.DefaultAmountArgumentValue, "number of results to return")
	runsListCmd.Flags().String("after", "", "show results after this value (used for pagination)")
	runsListCmd.Flags().String("branch", "", "show results for specific branch")
	runsListCmd.Flags().String("commit", "", "show results for specific commit ID")
}
