package cmd

import (
	"context"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd_utils"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/uri"
)

const commitsTemplate = `
{{ range $val := .Commits }}
ID: {{ $val.ID|yellow }}{{if $val.Committer }}
Author: {{ $val.Committer }}{{end}}
Date: {{ $val.CreationDate|date }}
	{{ if gt ($val.Parents|len) 1 -}}
Merge: {{ $val.Parents|join ", "|bold }}
	{{ end }}
	{{ $val.Message }}
	
	{{ range $key, $value := $val.Metadata }}
		{{ $key }} = {{ $value }}
	{{ end -}}
{{ end }}
{{.Pagination | paginate }}
`

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:   "log <branch uri>",
	Short: "show log of commits for the given branch",
	Args: cmd_utils.ValidationChain(
		cmd_utils.HasNArgs(1),
		cmd_utils.PositionValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		amount, err := cmd.Flags().GetInt("amount")
		if err != nil {
			DieErr(err)
		}
		after, err := cmd.Flags().GetString("after")
		if err != nil {
			DieErr(err)
		}
		client := getClient()
		branchURI := uri.Must(uri.Parse(args[0]))
		commits, pagination, err := client.GetCommitLog(context.Background(), branchURI.Repository, branchURI.Ref, after, amount)
		ctx := struct {
			Commits    []*models.Commit
			Pagination *Pagination
		}{
			Commits: commits,
		}
		if pagination != nil && swag.BoolValue(pagination.HasMore) {
			ctx.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}
		if err != nil {
			DieErr(err)
		}
		Write(commitsTemplate, ctx)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(logCmd)
	logCmd.Flags().Int("amount", -1, "how many results to return, or-1 for all results (used for pagination)")
	logCmd.Flags().String("after", "", "show results after this value (used for pagination)")
}
