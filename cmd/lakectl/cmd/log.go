package cmd

import (
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"golang.org/x/exp/slices"
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

func (d *dotWriter) Write(commits []apigen.Commit) {
	repoID := url.PathEscape(d.repositoryID)
	for _, commit := range commits {
		isMerge := len(commit.Parents) > 1
		label := fmt.Sprintf("%s<br/> %s", commit.Id[:8], html.EscapeString(commit.Message))
		if isMerge {
			label = fmt.Sprintf("<b>%s</b>", label)
		}
		baseURL := strings.TrimSuffix(strings.TrimSuffix(
			string(cfg.Server.EndpointURL), apiutil.BaseURL), "/")
		_, _ = fmt.Fprintf(d.w, "\n\t\"%s\" [shape=note target=\"_blank\" href=\"%s/repositories/%s/commits/%s\" label=< %s >]\n",
			commit.Id, baseURL, repoID, commit.Id, label)
		for _, parent := range commit.Parents {
			_, _ = fmt.Fprintf(d.w, "\t\"%s\" -> \"%s\";\n", parent, commit.Id)
		}
	}
}

// filter merge commits, used for --no-merges flag
func filterMergeCommits(commits []apigen.Commit) []apigen.Commit {
	filteredCommits := make([]apigen.Commit, 0, len(commits))

	// iterating through data.Commit, appending every instance with 1 or less parents.
	for _, commit := range commits {
		if len(commit.Parents) <= 1 {
			filteredCommits = append(filteredCommits, commit)
		}
	}
	return filteredCommits
}

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:               "log <branch URI>",
	Short:             "Show log of commits",
	Long:              "Show log of commits for a given branch",
	Example:           "lakectl log --dot lakefs://example-repository/main | dot -Tsvg > graph.svg",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		amount := Must(cmd.Flags().GetInt("amount"))
		after := Must(cmd.Flags().GetString("after"))
		limit := Must(cmd.Flags().GetBool("limit"))
		since := Must(cmd.Flags().GetString("since"))
		dot := Must(cmd.Flags().GetBool("dot"))
		noMerges := Must(cmd.Flags().GetBool("no-merges"))
		firstParent := Must(cmd.Flags().GetBool("first-parent"))
		objects := Must(cmd.Flags().GetStringSlice("objects"))
		prefixes := Must(cmd.Flags().GetStringSlice("prefixes"))
		stopAt := Must(cmd.Flags().GetString("stop-at"))

		if slices.Contains(objects, "") {
			Die("Objects list contains empty string!", 1)
		}
		if slices.Contains(prefixes, "") {
			Die("Prefixes list contains empty string!", 1)
		}

		pagination := apigen.Pagination{HasMore: true}
		showMetaRangeID := Must(cmd.Flags().GetBool("show-meta-range-id"))
		client := getClient()
		branchURI := MustParseBranchURI("branch URI", args[0])
		amountForPagination := amount
		if amountForPagination <= 0 {
			amountForPagination = internalPageSize
		}
		// case --no-merges & --amount, ask for more results since some will filter out
		if noMerges && amountForPagination < maxAmountNoMerges {
			amountForPagination *= noMergesHeuristic
		}
		logCommitsParams := &apigen.LogCommitsParams{
			After:       apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount:      apiutil.Ptr(apigen.PaginationAmount(amountForPagination)),
			Limit:       &limit,
			FirstParent: &firstParent,
			StopAt:      &stopAt,
		}
		if len(objects) > 0 {
			logCommitsParams.Objects = &objects
		}
		if len(prefixes) > 0 {
			logCommitsParams.Prefixes = &prefixes
		}
		if since != "" {
			sinceParsed, err := time.Parse(time.RFC3339, since)
			if err != nil {
				DieFmt("Failed to parse 'since' - %s", err)
			}
			logCommitsParams.Since = &sinceParsed
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
			logCommitsParams.After = apiutil.Ptr(apigen.PaginationAfter(pagination.NextOffset))
			data := struct {
				Commits         []apigen.Commit
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

			// case --no-merges, filter commits and subtract that amount from amount desired
			if noMerges {
				data.Commits = filterMergeCommits(data.Commits)
				// case user asked for a specified amount
				if amount > 0 {
					data.Commits = data.Commits[:amount]
				}
			}
			amount -= len(data.Commits)

			if dot {
				graph.Write(data.Commits)
			} else {
				Write(commitsTemplate, data)
			}

			if amount <= 0 {
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
	logCmd.Flags().Bool("no-merges", false, "skip merge commits")
	logCmd.Flags().Bool("show-meta-range-id", false, "also show meta range ID")
	logCmd.Flags().StringSlice("objects", nil, "show results that contains changes to at least one path in that list of objects. Use comma separator to pass all objects together")
	logCmd.Flags().StringSlice("prefixes", nil, "show results that contains changes to at least one path in that list of prefixes. Use comma separator to pass all prefixes together")
	logCmd.Flags().String("since", "", "show results since this date-time (RFC3339 format)")
	logCmd.Flags().String("stop-at", "", "a Ref to stop at (included in results)")
}
