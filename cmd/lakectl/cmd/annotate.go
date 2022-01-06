package cmd

import (
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
		res, err := client.ListObjectsWithResponse(context, pathURI.Repository, pathURI.Ref, &api.ListObjectsParams{Prefix: &pfx})
		DieOnResponseError(res, err)
		var from string
		for {
			params := &api.ListObjectsParams{
				Prefix: &pfx,
				After:  api.PaginationAfterPtr(from),
			}
			resp, err := client.ListObjectsWithResponse(context, pathURI.Repository, pathURI.Ref, params)
			DieOnResponseError(resp, err)
			for _, obj := range resp.JSON200.Results {
				logCommitsParams := &api.LogCommitsParams{
					Amount:  api.PaginationAmountPtr(1),
					Objects: &[]string{obj.Path},
				}
				res, err := client.LogCommitsWithResponse(context, pathURI.Repository, pathURI.Ref, logCommitsParams)
				DieOnResponseError(res, err)
				data := objectCommitData{
					Object: obj.Path,
				}
				if len(res.JSON200.Results) > 0 {
					data.Commit = res.JSON200.Results[0]
					data.CommitMessage = splitOnNewLine(stringTrimLen((res.JSON200.Results[0].Message), annotateMessageSize))
				}
				Write(annotateTemplate, data)
			}
			pagination := resp.JSON200.Pagination
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
