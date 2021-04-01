package cmd

import (
	"context"
	"fmt"
	"net/http"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/uri"
)

const actionRunResultTemplate = `
{{ . | table -}}
`

const actionTaskResultTemplate = `{{ $r := . }}
{{ range $idx, $val := .Hooks }}{{ index $r.HooksTable $idx | table -}}
{{ printf $val.HookRunId | call $r.HookLog }}
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
		ctx := cmd.Context()

		// run result information
		runsRes, err := client.GetRunWithResponse(ctx, u.Repository, runID)
		DieOnResponseError(runsRes, err)

		runResult := runsRes.JSON200
		Write(actionRunResultTemplate, convertRunResultTable(runResult))

		// iterator over hooks - print information and output
		runHooksRes, err := client.ListRunHooksWithResponse(ctx, u.Repository, runID, &api.ListRunHooksParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(runHooksRes, err)

		response := runHooksRes.JSON200.Results
		pagination := runHooksRes.JSON200.Pagination
		data := struct {
			Hooks      []api.HookRun
			HooksTable []*Table
			HookLog    func(hookRunID string) (string, error)
			Pagination *Pagination
		}{
			Hooks:      response,
			HooksTable: convertHookResultsTables(response),
			HookLog:    makeHookLog(ctx, client, u.Repository, runID),
		}
		if pagination.HasMore {
			data.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}
		Write(actionTaskResultTemplate, data)
	},
}

func makeHookLog(ctx context.Context, client api.ClientWithResponsesInterface, repositoryID string, runID string) func(hookRunID string) (string, error) {
	return func(hookRunID string) (string, error) {
		res, err := client.GetRunHookOutputWithResponse(ctx, repositoryID, runID, hookRunID)
		if err != nil {
			return "", err
		}
		if res.StatusCode() != http.StatusOK {
			if res.JSONDefault != nil {
				return "", fmt.Errorf("%w: %s", ErrRequestFailed, res.JSONDefault.Message)
			}
			return "", fmt.Errorf("%w: status code %d", ErrRequestFailed, res.StatusCode())
		}
		return string(res.Body), nil
	}
}

func convertRunResultTable(r *api.ActionRun) *Table {
	runID := text.FgYellow.Sprint(r.RunId)
	statusColor := text.FgRed
	if r.Status == "completed" {
		statusColor = text.FgGreen
	}
	status := statusColor.Sprint(r.Status)
	return &Table{
		Headers: []interface{}{"Run ID", "Event", "Branch", "Start Time", "End Time", "Commit ID", "Status"},
		Rows: [][]interface{}{
			{runID, r.EventType, r.Branch, r.StartTime, r.EndTime, r.CommitId, status},
		},
	}
}

func convertHookResultsTables(results []api.HookRun) []*Table {
	tables := make([]*Table, len(results))
	for i, r := range results {
		hookRunID := text.FgYellow.Sprint(r.HookRunId)
		statusColor := text.FgRed
		if r.Status == "completed" {
			statusColor = text.FgGreen
		}
		status := statusColor.Sprint(r.Status)
		tables[i] = &Table{
			Headers: []interface{}{"Hook Run ID", "Hook ID", "Start Time", "End Time", "Action", "Status"},
			Rows: [][]interface{}{
				{hookRunID, r.HookId, r.StartTime, r.EndTime, r.Action, status},
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
