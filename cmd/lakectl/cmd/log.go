package cmd

import (
	"net/http"

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
		limit := MustBool(cmd.Flags().GetBool("limit"))
		objectsList := MustSliceNonEmptyString("objects", MustStringSlice(cmd.Flags().GetStringSlice("objects")))
		prefixesList := MustSliceNonEmptyString("prefixes", MustStringSlice(cmd.Flags().GetStringSlice("prefixes")))

		pagination := api.Pagination{HasMore: true}
		showMetaRangeID, _ := cmd.Flags().GetBool("show-meta-range-id")
		client := getClient()
		branchURI := MustParseRefURI("branch", args[0])
		amountForPagination := amount
		if amountForPagination <= 0 {
			amountForPagination = internalPageSize
		}
		logCommitsParams := &api.LogCommitsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amountForPagination),
			Limit:  &limit,
		}
		if len(objectsList) > 0 {
			logCommitsParams.Objects = &objectsList
		}
		if len(prefixesList) > 0 {
			logCommitsParams.Prefixes = &prefixesList
		}
		for pagination.HasMore {
			resp, err := client.LogCommitsWithResponse(cmd.Context(), branchURI.Repository, branchURI.Ref, logCommitsParams)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			pagination = resp.JSON200.Pagination
			logCommitsParams.After = api.PaginationAfterPtr(pagination.NextOffset)
			data := struct {
				Commits         []api.Commit
				Pagination      *Pagination
				ShowMetaRangeID bool
			}{
				Commits:         resp.JSON200.Results,
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
	logCmd.Flags().Int("amount", 0, "number of results to return. By default, all results are returned")
	logCmd.Flags().Bool("limit", false, "limit result just to amount. By default, returns whether more items are available.")
	logCmd.Flags().String("after", "", "show results after this value (used for pagination)")
	logCmd.Flags().Bool("show-meta-range-id", false, "also show meta range ID")
	logCmd.Flags().StringSlice("objects", nil, "show results that contains changes to at least one path in that list of objects. Use comma separator to pass all objects together")
	logCmd.Flags().StringSlice("prefixes", nil, "show results that contains changes to at least one path in that list of prefixes. Use comma separator to pass all prefixes together")
}
