package cmd

import (
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var fsLsCmd = &cobra.Command{
	Use:               "ls <path URI>",
	Short:             "List entries under a given tree",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		pathURI := MustParsePathURI("path URI", args[0])
		recursive := Must(cmd.Flags().GetBool(recursiveFlagName))
		prefix := *pathURI.Path

		// prefix we need to trim in ls output (non-recursive)
		var trimPrefix string
		if idx := strings.LastIndex(prefix, PathDelimiter); idx != -1 {
			trimPrefix = prefix[:idx+1]
		}
		// delimiter used for listing
		var paramsDelimiter apigen.PaginationDelimiter
		if !recursive {
			paramsDelimiter = PathDelimiter
		}
		var from string
		for {
			pfx := prefix
			params := &apigen.ListObjectsParams{
				Prefix:    &pfx,
				After:     &from,
				Delimiter: &paramsDelimiter,
			}
			resp, err := client.ListObjectsWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, params)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}

			results := resp.JSON200.Results
			// trim prefix if non-recursive
			if !recursive {
				for i := range results {
					trimmed := strings.TrimPrefix(results[i].Path, trimPrefix)
					results[i].Path = trimmed
				}
			}

			Write(fsLsTemplate, results)
			pagination := resp.JSON200.Pagination
			if !pagination.HasMore {
				break
			}
			from = pagination.NextOffset
		}
	},
}

//nolint:gochecknoinits
func init() {
	withRecursiveFlag(fsLsCmd, "list all objects under the specified path")
	fsCmd.AddCommand(fsLsCmd)
}

const fsLsTemplate = `{{ range $val := . -}}
{{ $val.PathType|ljust 12 }}    {{ if eq $val.PathType "object" }}{{ $val.Mtime|date|ljust 29 }}    {{ $val.SizeBytes|human_bytes|ljust 12 }}{{ else }}                                            {{ end }}    {{ $val.Path|yellow }}
{{ end -}}
`
