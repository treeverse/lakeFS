package cmd

import (
	"bytes"
	"context"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

const actionRunResultTemplate = `
Run ID: {{ .RunID }}
	Event: {{ .EventType }}
	Branch: {{ .Branch }}
	Start time: {{ .StartTime }}
	End time: {{ .EndTime }}
{{if .CommitID}}	Commit ID: {{ .CommitID }}{{end}}
	Status: {{ .Status }}
`

const actionTaskResultTemplate = `{{ $log := .HookLog }}
{{ range $val := .Hooks }}
Hook Run ID: {{ $val.HookRunID }}
	Hook ID: {{ $val.HookID }}
	Start time: {{ $val.StartTime }}
	End time: {{ $val.EndTime }}
	Action: {{ $val.Action }}
	Status: {{ $val.Status }}
Output:
{{ printf $val.HookRunID | call $log }}
{{ end }}{{ .Pagination | paginate }}
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
		Write(actionRunResultTemplate, runResult)

		// iterator over hooks - print information and output
		response, pagination, err := client.ListRunTaskResults(ctx, u.Repository, runID, after, amount)
		if err != nil {
			DieErr(err)
		}
		data := struct {
			Hooks      []*models.HookRun
			HookLog    func(hookRunID string) (string, error)
			Pagination *Pagination
		}{
			Hooks: response,
			HookLog: func(hookRunID string) (string, error) {
				// return output content
				var buf bytes.Buffer
				err := client.GetRunHookOutput(ctx, u.Repository, runID, hookRunID, &buf)
				if err != nil {
					return "", err
				}
				return buf.String(), nil
			},
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

//nolint:gochecknoinits
func init() {
	actionsRunsCmd.AddCommand(runsDescribeCmd)
	runsDescribeCmd.Flags().Int("amount", -1, "how many results to return, or '-1' for default (used for pagination)")
	runsDescribeCmd.Flags().String("after", "", "show results after this value (used for pagination)")
}
