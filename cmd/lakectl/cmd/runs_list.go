package cmd

import (
	"context"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

const actionsRunsListTemplate = `{{.ActionsRunsTable | table -}}
{{.Pagination | paginate }}
`

var runsListCmd = &cobra.Command{
	Use:     "list",
	Short:   "List runs",
	Long:    `List all runs on a repository or a specific branch`,
	Example: "lakectl actions runs list lakefs://<repository>[@branch]",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.Or(
			cmdutils.FuncValidator(0, uri.ValidateRepoURI),
			cmdutils.FuncValidator(0, uri.ValidateRefURI),
		),
	),
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		u := uri.Must(uri.Parse(args[0]))

		var branchOptional *string
		if u.Ref != "" {
			branchOptional = &u.Ref
		}
		client := getClient()
		ctx := context.Background()
		response, pagination, err := client.ListRunResults(ctx, u.Repository, branchOptional, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(response))
		for i, row := range response {
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
	runsListCmd.Flags().Int("amount", -1, "how many results to return, or-1 for all results (used for pagination)")
	runsListCmd.Flags().String("after", "", "show results after this value (used for pagination)")
}
