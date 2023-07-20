package cmd

import (
	"context"
	"net/http"

	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
)

const actionRunResultTemplate = `{{ . | table -}}`

const actionTaskResultTemplate = `{{ $r := . }}{{ range $idx, $val := .Hooks }}{{ index $r.HooksTable $idx | table -}}{{ printf $val.HookRunId | call $r.HookLog }}{{ end }}{{ if .Pagination }}
{{ .Pagination | paginate }}{{ end }}`

const runsShowRequiredArgs = 2

var runsDescribeCmd = &cobra.Command{
	Use:               "describe",
	Short:             "Describe run results",
	Long:              `Show information about the run and all the hooks that were executed as part of the run`,
	Example:           "lakectl actions runs describe lakefs://<repository> <run_id>",
	Args:              cobra.ExactArgs(runsShowRequiredArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		amount := utils.MustInt(cmd.Flags().GetInt("amount"))
		after := utils.MustString(cmd.Flags().GetString("after"))
		u := utils.MustParseRepoURI("repository", args[0])
		pagination := api.Pagination{HasMore: true}

		utils.Fmt("Repository: %s\n", u.String())
		runID := args[1]

		client := getClient()
		ctx := cmd.Context()

		// run result information
		runsRes, err := client.GetRunWithResponse(ctx, u.Repository, runID)
		utils.DieOnErrorOrUnexpectedStatusCode(runsRes, err, http.StatusOK)
		if runsRes.JSON200 == nil {
			utils.Die("Bad response from server", 1)
		}

		runResult := runsRes.JSON200
		utils.Write(actionRunResultTemplate, convertRunResultTable(runResult))
		for pagination.HasMore {
			amountForPagination := amount
			if amountForPagination <= 0 {
				amountForPagination = utils.InternalPageSize
			}
			// iterator over hooks - print information and output
			runHooksRes, err := client.ListRunHooksWithResponse(ctx, u.Repository, runID, &api.ListRunHooksParams{
				After:  api.PaginationAfterPtr(after),
				Amount: api.PaginationAmountPtr(amountForPagination),
			})
			utils.DieOnErrorOrUnexpectedStatusCode(runHooksRes, err, http.StatusOK)
			if runHooksRes.JSON200 == nil {
				utils.Die("Bad response from server", 1)
			}
			pagination = runHooksRes.JSON200.Pagination
			data := struct {
				Hooks      []api.HookRun
				HooksTable []*utils.Table
				HookLog    func(hookRunID string) (string, error)
				Pagination *utils.Pagination
			}{
				Hooks:      runHooksRes.JSON200.Results,
				HooksTable: convertHookResultsTables(runHooksRes.JSON200.Results),
				HookLog:    makeHookLog(ctx, client, u.Repository, runID),
				Pagination: &utils.Pagination{
					Amount:  amount,
					HasNext: pagination.HasMore,
					After:   pagination.NextOffset,
				},
			}
			utils.Write(actionTaskResultTemplate, data)
			after = pagination.NextOffset
			if amount != 0 {
				// user request only one page
				break
			}
		}
	},
}

func makeHookLog(ctx context.Context, client api.ClientWithResponsesInterface, repositoryID string, runID string) func(hookRunID string) (string, error) {
	return func(hookRunID string) (string, error) {
		resp, err := client.GetRunHookOutputWithResponse(ctx, repositoryID, runID, hookRunID)
		if err != nil {
			return "", err
		}
		if resp.StatusCode() != http.StatusOK {
			return "", helpers.ResponseAsError(resp)
		}
		return string(resp.Body), nil
	}
}

func convertRunResultTable(r *api.ActionRun) *utils.Table {
	runID := text.FgYellow.Sprint(r.RunId)
	statusColor := text.FgRed
	if r.Status == "completed" {
		statusColor = text.FgGreen
	}
	status := statusColor.Sprint(r.Status)
	return &utils.Table{
		Headers: []interface{}{"Run ID", "Event", "Branch", "Start Time", "End Time", "Commit ID", "Status"},
		Rows: [][]interface{}{
			{runID, r.EventType, r.Branch, r.StartTime, r.EndTime, r.CommitId, status},
		},
	}
}

func convertHookResultsTables(results []api.HookRun) []*utils.Table {
	tables := make([]*utils.Table, len(results))
	for i, r := range results {
		hookRunID := text.FgYellow.Sprint(r.HookRunId)
		statusColor := text.FgRed
		switch r.Status {
		case "completed":
			statusColor = text.FgGreen
		case "skipped":
			statusColor = text.FgYellow
		case "failed":
			statusColor = text.FgRed
		}
		status := statusColor.Sprint(r.Status)
		tables[i] = &utils.Table{
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
	runsDescribeCmd.Flags().Int("amount", 0, "number of results to return. By default, all results are returned.")
	runsDescribeCmd.Flags().String("after", "", "show results after this value (used for pagination)")
}
