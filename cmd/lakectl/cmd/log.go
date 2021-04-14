package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/uri"
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
		pagination := api.Pagination{
			HasMore: true,
		}
		showMetaRangeID, _ := cmd.Flags().GetBool("show-meta-range-id")
		client := getClient()
		branchURI := uri.Must(uri.Parse(args[0]))
		amountForPagination := amount
		if amountForPagination == -1 {
			amountForPagination = defaultPaginationAmount
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
			}
			if pagination.HasMore && amount != -1 {
				data.Pagination = &Pagination{
					Amount:  amount,
					HasNext: true,
					After:   pagination.NextOffset,
				}
				Write(commitsTemplate, data)
				break
			}
			Write(commitsTemplate, data)
		}

	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(logCmd)
	logCmd.Flags().Int("amount", -1, "number of results to return. By default, all results are returned.")
	logCmd.Flags().String("after", "", "show results after this value (used for pagination)")
	logCmd.Flags().Bool("show-meta-range-id", false, "also show meta range ID")
}
