package cmd

import (
	"bytes"
	"context"

	"github.com/go-openapi/swag"
	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

const actionRunResultTemplate = `
{{ . | table -}}
`

const actionTaskResultTemplate = `{{ $r := . }}
{{ range $idx, $val := .Hooks }}{{ index $r.HooksTable $idx | table -}}
{{ printf $val.HookRunID | call $r.HookLog }}
{{ end }}
{{ .Pagination | paginate }}
`

const runsShowRequiredArgs = 2

var runsDescribeCmd = &cobra.Command{
	Use:     "describe",
	Short:   "Describe run results",
	Long:    `Show information about the run and all the hooks that were executed as part of the run`,
	Example: "lakectl actions runs describe lakefs://<repository> <run_id>",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(runsShowRequiredArgs),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		u := uri.Must(uri.Parse(args[0]))
		runID := args[1]

		client := getClient()
		ctx := context.Background()

		// run result information
		runResult, err := client.GetRunResult(ctx, u.Repository, runID)
		if err != nil {
			DieErr(err)
		}
		Write(actionRunResultTemplate, convertRunResultTable(runResult))

		// iterator over hooks - print information and output
		response, pagination, err := client.ListRunTaskResults(ctx, u.Repository, runID, after, amount)
		if err != nil {
			DieErr(err)
		}
		data := struct {
			Hooks      []*models.HookRun
			HooksTable []*Table
			HookLog    func(hookRunID string) (string, error)
			Pagination *Pagination
		}{
			Hooks:      response,
			HooksTable: convertHookResultsTables(response),
			HookLog:    makeHookLog(ctx, client, u.Repository, runID),
		}
		if pagination != nil && swag.BoolValue(pagination.HasMore) {
			data.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}
		Write(actionTaskResultTemplate, data)
	},
}

func makeHookLog(ctx context.Context, client api.Client, repositoryID string, runID string) func(hookRunID string) (string, error) {
	return func(hookRunID string) (string, error) {
		var buf bytes.Buffer
		err := client.GetRunHookOutput(ctx, repositoryID, runID, hookRunID, &buf)
		if err != nil {
			return "", err
		}
		return buf.String(), nil
	}
}

func convertRunResultTable(r *models.ActionRun) *Table {
	runID := text.FgYellow.Sprint(swag.StringValue(r.RunID))
	branch := swag.StringValue(r.Branch)
	commitID := swag.StringValue(r.CommitID)
	statusColor := text.FgRed
	if r.Status == models.ActionRunStatusCompleted {
		statusColor = text.FgGreen
	}
	status := statusColor.Sprint(r.Status)
	return &Table{
		Headers: []interface{}{"Run ID", "Event", "Branch", "Start Time", "End Time", "Commit ID", "Status"},
		Rows: [][]interface{}{
			{runID, r.EventType, branch, r.StartTime, r.EndTime, commitID, status},
		},
	}
}

func convertHookResultsTables(results []*models.HookRun) []*Table {
	tables := make([]*Table, len(results))
	for i, r := range results {
		hookRunID := text.FgYellow.Sprint(swag.StringValue(r.HookRunID))
		statusColor := text.FgRed
		if r.Status == models.ActionRunStatusCompleted {
			statusColor = text.FgGreen
		}
		status := statusColor.Sprint(r.Status)
		tables[i] = &Table{
			Headers: []interface{}{"Hook Run ID", "Hook ID", "Start Time", "End Time", "Action", "Status"},
			Rows: [][]interface{}{
				{hookRunID, r.HookID, r.StartTime, r.EndTime, r.Action, status},
			},
		}
	}
	return tables
}

//nolint:gochecknoinits
func init() {
	actionsRunsCmd.AddCommand(runsDescribeCmd)
	runsDescribeCmd.Flags().Int("amount", -1, "how many results to return, or '-1' for default (used for pagination)")
	runsDescribeCmd.Flags().String("after", "", "show results after this value (used for pagination)")
}
