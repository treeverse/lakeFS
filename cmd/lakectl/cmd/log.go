package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/uri"
)

const commitsTemplate = `
{{ range $val := .Commits }}
ID:            {{ $val.Id|yellow }}{{if $val.Committer }}
Author:        {{ $val.Committer }}{{end}}
Date:          {{ $val.CreationDate|date }}
{{ if $.ShowMetaRangeID }}Meta Range ID: {{ $val.MetaRangeId }}
{{ end -}}
{{ if gt ($val.Parents|len) 1 -}}
Merge:         {{ $val.Parents|join ", "|bold }}
{{ end }}
	{{ $val.Message }}
	
	{{ range $key, $value := $val.Metadata.AdditionalProperties }}
		{{ $key }} = {{ $value }}
	{{ end -}}
{{ end }}
{{.Pagination | paginate }}
`

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:   "log <branch uri>",
	Short: "show log of commits for the given branch",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
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
		var pagination api.Pagination
		commits := make([]api.Commit, 0)
		showMetaRangeID, _ := cmd.Flags().GetBool("show-meta-range-id")
		client := getClient()
		branchURI := uri.Must(uri.Parse(args[0]))
		for {
			amountForPagination := amount
			if amountForPagination == -1 {
				amountForPagination = defaultPaginationAmount
			}
			res, err := client.LogCommitsWithResponse(cmd.Context(), branchURI.Repository, branchURI.Ref, &api.LogCommitsParams{
				After:  api.PaginationAfterPtr(after),
				Amount: api.PaginationAmountPtr(amountForPagination),
			})
			DieOnResponseError(res, err)
			commits = append(commits, res.JSON200.Results...)
			pagination = res.JSON200.Pagination
			if amount != -1 || !pagination.HasMore {
				break
			}
			after = pagination.NextOffset
		}
		data := struct {
			Commits         []api.Commit
			Pagination      *Pagination
			ShowMetaRangeID bool
		}{
			Commits:         commits,
			ShowMetaRangeID: showMetaRangeID,
		}
		if pagination.HasMore {
			data.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}
		Write(commitsTemplate, data)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(logCmd)
	logCmd.Flags().Int("amount", -1, "number of results to return. By default, all results are returned.")
	logCmd.Flags().String("after", "", "show results after this value (used for pagination)")
	logCmd.Flags().Bool("show-meta-range-id", false, "also show meta range ID")
}
