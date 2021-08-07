package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const commitsTemplate = `{{ range $val := .Commits }}
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
{{ end }}{{ if .Pagination  }}
{{.Pagination | paginate }}{{ end }}`

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:   "log <branch uri>",
	Short: "Show log of commits",
	Long:  "Show log of commits for a given branch",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		amount := MustInt(cmd.Flags().GetInt("amount"))
		after := MustString(cmd.Flags().GetString("after"))
		pagination := api.Pagination{HasMore: true}
		showMetaRangeID, _ := cmd.Flags().GetBool("show-meta-range-id")
		client := getClient()
		branchURI := MustParseRefURI("branch", args[0])
		amountForPagination := amount
		if amountForPagination <= 0 {
			amountForPagination = internalPageSize
		}
		for pagination.HasMore {
			res, err := client.LogCommitsWithResponse(cmd.Context(), branchURI.Repository, branchURI.Ref, &api.LogCommitsParams{
				After:  api.PaginationAfterPtr(after),
				Amount: api.PaginationAmountPtr(amountForPagination),
			})
			DieOnResponseError(res, err)
			pagination = res.JSON200.Pagination
			after = pagination.NextOffset
			data := struct {
				Commits         []api.Commit
				Pagination      *Pagination
				ShowMetaRangeID bool
			}{
				Commits:         res.JSON200.Results,
				ShowMetaRangeID: showMetaRangeID,
				Pagination: &Pagination{
					Amount:  amount,
					HasNext: pagination.HasMore,
					After:   pagination.NextOffset,
				},
			}
			Write(commitsTemplate, data)
			if amount != 0 {
				// user request only one page
				break
			}
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(logCmd)
	logCmd.Flags().Int("amount", 0, "number of results to return. By default, all results are returned.")
	logCmd.Flags().String("after", "", "show results after this value (used for pagination)")
	logCmd.Flags().Bool("show-meta-range-id", false, "also show meta range ID")
}
