package cmd

import (
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
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

type dotWriter struct {
	w            io.Writer
	repositoryID string
}

func (d *dotWriter) Start() {
	_, _ = fmt.Fprintf(d.w, "digraph {\n\trankdir=\"BT\"\n")
}

func (d *dotWriter) End() {
	_, _ = fmt.Fprint(d.w, "\n}\n")
}

func (d *dotWriter) Write(commits []api.Commit) {
	repoID := url.PathEscape(d.repositoryID)
	for _, commit := range commits {
		isMerge := len(commit.Parents) > 1
		label := fmt.Sprintf("%s<br/> %s", commit.Id[:8], html.EscapeString(commit.Message))
		if isMerge {
			label = fmt.Sprintf("<b>%s</b>", label)
		}
		baseURL := strings.TrimSuffix(strings.TrimSuffix(
			cfg.Values.Server.EndpointURL, "/api/v1"), "/")
		_, _ = fmt.Fprintf(d.w, "\n\t\"%s\" [shape=note target=\"_blank\" href=\"%s/repositories/%s/commits/%s\" label=< %s >]\n",
			commit.Id, baseURL, repoID, commit.Id, label)
		for _, parent := range commit.Parents {
			_, _ = fmt.Fprintf(d.w, "\t\"%s\" -> \"%s\";\n", parent, commit.Id)
		}
	}
}

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:               "log <branch uri>",
	Short:             "Show log of commits",
	Long:              "Show log of commits for a given branch",
	Example:           "lakectl log --dot lakefs://example-repository/main | dot -Tsvg > graph.svg",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		amount := MustInt(cmd.Flags().GetInt("amount"))
		after := MustString(cmd.Flags().GetString("after"))
		limit := MustBool(cmd.Flags().GetBool("limit"))
		dot := MustBool(cmd.Flags().GetBool("dot"))
		firstParent := MustBool(cmd.Flags().GetBool("first-parent"))
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
			After:       api.PaginationAfterPtr(after),
			Amount:      api.PaginationAmountPtr(amountForPagination),
			Limit:       &limit,
			FirstParent: &firstParent,
		}
		if len(objectsList) > 0 {
			logCommitsParams.Objects = &objectsList
		}
		if len(prefixesList) > 0 {
			logCommitsParams.Prefixes = &prefixesList
		}

		graph := &dotWriter{
			w:            os.Stdout,
			repositoryID: branchURI.Repository,
		}
		if dot {
			graph.Start()
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
				graph.Write(data.Commits)
			} else {
				Write(commitsTemplate, data)
			}

			if amount != 0 {
				// user request only one page
				break
			}
		}

		if dot {
			graph.End()
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
	logCmd.Flags().Bool("first-parent", false, "follow only the first parent commit upon seeing a merge commit")
	logCmd.Flags().Bool("show-meta-range-id", false, "also show meta range ID")
	logCmd.Flags().StringSlice("objects", nil, "show results that contains changes to at least one path in that list of objects. Use comma separator to pass all objects together")
	logCmd.Flags().StringSlice("prefixes", nil, "show results that contains changes to at least one path in that list of prefixes. Use comma separator to pass all prefixes together")
}
