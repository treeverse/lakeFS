package cmd

import (
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	annotateTemplate = `{{  $val := .Commit}}
{{ $.Object|ljust 15}} {{ $val.Committer|ljust 20 }} {{ $val.Id | printf "%.16s"|ljust 20 }} {{if $val.CreationDate}}{{ $val.CreationDate|date}}{{end}}  {{ $.CommitMessage |ljust 30 }}
`
	annotateMessageSize = 200
	ellipsis            = "..."
)

type objectCommitData struct {
	Object        string
	Commit        api.Commit
	CommitMessage string
}

var annotateCmd = &cobra.Command{
	Use:     "annotate <path uri>",
	Short:   "List entries under a given path, annotating each with the latest modifying commit",
	Aliases: []string{"blame"},
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path", args[0])
		client := getClient()
		pfx := api.PaginationPrefix(*pathURI.Path)
		context := cmd.Context()
		resp, err := client.ListObjectsWithResponse(context, pathURI.Repository, pathURI.Ref, &api.ListObjectsParams{Prefix: &pfx})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		var from string
		for {
			params := &api.ListObjectsParams{
				Prefix: &pfx,
				After:  api.PaginationAfterPtr(from),
			}
			listObjectsResp, err := client.ListObjectsWithResponse(context, pathURI.Repository, pathURI.Ref, params)
			DieOnErrorOrUnexpectedStatusCode(listObjectsResp, err, http.StatusOK)
			for _, obj := range resp.JSON200.Results {
				logCommitsParams := &api.LogCommitsParams{
					Amount:  api.PaginationAmountPtr(1),
					Objects: &[]string{obj.Path},
				}
				logCommitsResp, err := client.LogCommitsWithResponse(context, pathURI.Repository, pathURI.Ref, logCommitsParams)
				DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
				data := objectCommitData{
					Object: obj.Path,
				}
				if len(logCommitsResp.JSON200.Results) > 0 {
					data.Commit = logCommitsResp.JSON200.Results[0]
					data.CommitMessage = splitOnNewLine(stringTrimLen((logCommitsResp.JSON200.Results[0].Message), annotateMessageSize))
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
	return strings.SplitN(str, "\\n", 2)[0]
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(annotateCmd)
}
