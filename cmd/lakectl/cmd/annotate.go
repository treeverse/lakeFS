package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const annotateTemplate = `{{  $val := .Commit}}
{{ $.Object|ljust 15}} {{ $val.Committer|ljust 20 }} {{ $val.Id | printf "%.16s"|ljust 20 }} {{ $val.CreationDate|date }}  {{ $.CommitMessage |ljust 30 }}
`
const messageSize = 100
const trailingThreeDots = "..."

var annotateCmd = &cobra.Command{
	Use:     "annotate <path uri>",
	Short:   "List entries under a given path, annotating each with the latest modifying commit ",
	Aliases: []string{"blame"},
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path", args[0])
		client := getClient()
		amount := 1
		pfx := api.PaginationPrefix(*pathURI.Path)
		context := cmd.Context()
		res, err := client.ListObjectsWithResponse(context, pathURI.Repository, pathURI.Ref, &api.ListObjectsParams{Prefix: &pfx})
		DieOnResponseError(res, err)
		var from string
		for {
			pfx := api.PaginationPrefix(*pathURI.Path)
			params := &api.ListObjectsParams{
				Prefix: &pfx,
				After:  api.PaginationAfterPtr(from),
			}
			resp, err := client.ListObjectsWithResponse(context, pathURI.Repository, pathURI.Ref, params)
			DieOnResponseError(resp, err)
			for _, obj := range resp.JSON200.Results {
				prfx := []string{obj.Path}
				logCommitsParams := &api.LogCommitsParams{
					Amount:  api.PaginationAmountPtr(amount),
					Objects: &prfx,
				}
				res, err := client.LogCommitsWithResponse(context, pathURI.Repository, pathURI.Ref, logCommitsParams)
				DieOnResponseError(res, err)
				if len(res.JSON200.Results) > 0 {
					data := struct {
						Commit        api.Commit
						Object        string
						CommitMessage string
					}{
						Commit:        res.JSON200.Results[0],
						Object:        obj.Path,
						CommitMessage: splitOnNewLine(stringTrimLen((res.JSON200.Results[0].Message), messageSize)),
					}
					Write(annotateTemplate, data)
				} else {
					fmt.Println("Error, No commit info has been retrieved from the api call")
				}
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
	if len(str) > size && size > len(trailingThreeDots) {
		str = str[:size-len(trailingThreeDots)] + trailingThreeDots
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
