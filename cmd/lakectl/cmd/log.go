package cmd

import (
	"context"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/uri"
)

var commitsTemplate = `{{ range $val := .Commits }}
{{ if gt  ($val.Parents|len) 0 -}}
commit {{ $val.ID|yellow }}
Author: {{ $val.Committer }}
Date: {{ $val.CreationDate|date }}
{{ if gt ($val.Parents|len) 1 -}}
Merge: {{ $val.Parents|join ", "|bold }}
{{ end }}

    {{ $val.Message }}

    {{ range $key, $value := $val.Metadata }}
    {{ $key }} = {{ $value }}
	{{ end -}}
{{ end -}}
{{ end }}
{{.Pagination | paginate }}
`

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:   "log [branch uri]",
	Short: "show log of commits for the given branch",
	Args: ValidationChain(
		HasNArgs(1),
		IsRefURI(0),
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
			commits,
			nil,
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

func init() {
	rootCmd.AddCommand(logCmd)
	logCmd.Flags().Int("amount", -1, "how many results to return, or-1 for all results (used for pagination)")
	logCmd.Flags().String("after", "", "show results after this value (used for pagination)")
}
