package cmd

import (
	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
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

		u := uri.Must(uri.Parse(args[0]))
		if commit != "" && branch != "" {
			Die("Can't specify 'commit' and 'branch'", 1)
		}

		client := getClient()
		ctx := cmd.Context()
		var err error
		var results []*models.ActionRun
		var pagination *models.Pagination

		// list runs with optional branch filter
		var optionalBranch *string
		if branch != "" {
			optionalBranch = &branch
		}
		var optionalCommit *string
		if commit != "" {
			optionalCommit = &commit
		}

		results, pagination, err = client.ListRunResults(ctx, u.Repository, optionalBranch, optionalCommit, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(results))
		for i, row := range results {
			rows[i] = []interface{}{
				swag.StringValue(row.RunID),
				row.EventType,
				row.StartTime,
				row.EndTime,
				swag.StringValue(row.Branch),
				swag.StringValue(row.CommitID),
				row.Status,
			}
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
		if pagination != nil && swag.BoolValue(pagination.HasMore) {
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
	runsListCmd.Flags().Int("amount", -1, "how many results to return, or '-1' for default (used for pagination)")
	runsListCmd.Flags().String("after", "", "show results after this value (used for pagination)")
	runsListCmd.Flags().String("branch", "", "show results for specific branch")
	runsListCmd.Flags().String("commit", "", "show results for specific commit ID")
}
