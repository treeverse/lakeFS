package cmd

import (
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

const (
	annotateTemplate = `{{  $val := .Commit}}{{ $.Object|ljust 15}} {{ $val.Committer|ljust 20 }} {{ $val.Id | printf "%.16s"|ljust 20 }} {{if $val.CreationDate}}{{ $val.CreationDate|date}}{{end}}  {{ $.CommitMessage }}
`
	annotateMessageSize = 200
	ellipsis            = "..."
)

type objectCommitData struct {
	Object        string
	Commit        apigen.Commit
	CommitMessage string
}

var annotateCmd = &cobra.Command{
	Use:               "annotate <path URI>",
	Short:             "List entries under a given path, annotating each with the latest modifying commit",
	Aliases:           []string{"blame"},
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path URI", args[0])
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		firstParent := Must(cmd.Flags().GetBool("first-parent"))
		client := getClient()
		pfx := apigen.PaginationPrefix(*pathURI.Path)
		context := cmd.Context()
		resp, err := client.ListObjectsWithResponse(context, pathURI.Repository, pathURI.Ref, &apigen.ListObjectsParams{Prefix: &pfx})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		var listObjectsDelimiter apigen.PaginationDelimiter
		if !recursive {
			listObjectsDelimiter = PathDelimiter
		}
		var from string
		limit := true
		for {
			params := &apigen.ListObjectsParams{
				Prefix:    &pfx,
				After:     apiutil.Ptr(apigen.PaginationAfter(from)),
				Delimiter: &listObjectsDelimiter,
			}
			listObjectsResp, err := client.ListObjectsWithResponse(context, pathURI.Repository, pathURI.Ref, params)
			DieOnErrorOrUnexpectedStatusCode(listObjectsResp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			for _, obj := range listObjectsResp.JSON200.Results {
				logCommitsParams := &apigen.LogCommitsParams{
					Amount:      apiutil.Ptr(apigen.PaginationAmount(1)),
					Limit:       &limit,
					FirstParent: &firstParent,
				}
				if recursive {
					logCommitsParams.Objects = &[]string{obj.Path}
				} else {
					logCommitsParams.Prefixes = &[]string{obj.Path}
				}
				logCommitsResp, err := client.LogCommitsWithResponse(context, pathURI.Repository, pathURI.Ref, logCommitsParams)
				DieOnErrorOrUnexpectedStatusCode(logCommitsResp, err, http.StatusOK)
				if resp.JSON200 == nil {
					Die("Bad response from server", 1)
				}
				data := objectCommitData{
					Object: obj.Path,
				}
				if len(logCommitsResp.JSON200.Results) > 0 {
					data.Commit = logCommitsResp.JSON200.Results[0]
					data.CommitMessage = splitOnNewLine(stringTrimLen(logCommitsResp.JSON200.Results[0].Message, annotateMessageSize))
				}
				Write(annotateTemplate, data)
			}
			pagination := listObjectsResp.JSON200.Pagination
			if !pagination.HasMore {
				break
			}
			from = pagination.NextOffset
		}
	},
}

func stringTrimLen(str string, size int) string {
	if len(str) > size && size > len(ellipsis) {
		str = str[:size-len(ellipsis)] + ellipsis
	}
	return str
}

func splitOnNewLine(str string) string {
	return strings.SplitN(str, "\\n", 2)[0] //nolint: mnd
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(annotateCmd)
	withRecursiveFlag(annotateCmd, "recursively annotate all entries under a given path or prefix")
	annotateCmd.Flags().Bool("first-parent", false, "follow only the first parent commit upon seeing a merge commit")
}
