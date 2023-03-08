package cmd

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

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
{{ if $val.Metadata.AdditionalProperties }}
Metadata:
	{{ range $key, $value := $val.Metadata.AdditionalProperties }}
	{{ $key | printf "%-18s" }} = {{ $value }}
	{{ end -}}
{{ end -}}
{{ end }}{{ if .Pagination  }}
{{.Pagination | paginate }}{{ end }}`

// escapeDot escapes a string to match the graphviz dot-graph syntax
// based on: https://graphviz.org/doc/info/lang.html
func escapeDot(s string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(s))
	return buf.String()
}

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:               "log <branch uri>",
	Short:             "Show log of commits",
	Long:              "Show log of commits for a given branch",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		amount := MustInt(cmd.Flags().GetInt("amount"))
		after := MustString(cmd.Flags().GetString("after"))
		limit := MustBool(cmd.Flags().GetBool("limit"))
		dot := MustBool(cmd.Flags().GetBool("dot"))
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

		if dot {
			fmt.Printf("digraph {\n\trankdir=\"BT\"\n")
		}

		for pagination.HasMore {
			resp, err := client.LogCommitsWithResponse(cmd.Context(), branchURI.Repository, branchURI.Ref, logCommitsParams)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
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
			if dot {
				for _, commit := range resp.JSON200.Results {
					isMerge := len(commit.Parents) > 1
					label := fmt.Sprintf("%s<br/> %s", commit.Id[:8], escapeDot(commit.Message))
					if isMerge {
						label = fmt.Sprintf("<b>%s</b>", label)
					}
					url := strings.TrimSuffix(strings.TrimSuffix(
						cfg.Values.Server.EndpointURL, "/api/v1"), "/")
					fmt.Printf("\n\t\"%s\" [shape=note target=\"_blank\" href=\"%s/repositories/%s/commits/%s\" label=< %s >]\n",
						commit.Id, url, branchURI.Repository, commit.Id, label)
					for _, parent := range commit.Parents {
						fmt.Printf("\t\"%s\" -> \"%s\";\n", parent, commit.Id)
					}
				}
			} else {
				Write(commitsTemplate, data)
			}
			if amount != 0 {
				// user request only one page
				break
			}
		}

		if dot {
			fmt.Print("\n}\n")
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(logCmd)
	logCmd.Flags().Int("amount", 0, "number of results to return. By default, all results are returned")
	logCmd.Flags().Bool("limit", false, "limit result just to amount. By default, returns whether more items are available.")
	logCmd.Flags().String("after", "", "show results after this value (used for pagination)")
	logCmd.Flags().Bool("dot", false, "return results in a dotgraph format")
	logCmd.Flags().Bool("show-meta-range-id", false, "also show meta range ID")
	logCmd.Flags().StringSlice("objects", nil, "show results that contains changes to at least one path in that list of objects. Use comma separator to pass all objects together")
	logCmd.Flags().StringSlice("prefixes", nil, "show results that contains changes to at least one path in that list of prefixes. Use comma separator to pass all prefixes together")
}
