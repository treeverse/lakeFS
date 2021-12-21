package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const annotateTemplate = `{{ range $val := .Commits}}
{{ $.Object|ljust 10}} {{ $val.Committer }} {{ $val.Id | printf "%.16s"|ljust 20 }} {{ $val.Message |ljust 30 }} {{ $val.CreationDate|date }}{{ end -}}
`

var annotateCmd = &cobra.Command{
	Use:   "annotate <path uri>",
	Short: "Show who created the latest commit",
	Long:  "Show who created the latest commit to all objects in a given branch",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, c_args []string) {
		pathURI := MustParsePathURI("path", c_args[0])
		client := getClient()
		amount := 1
		pfx := api.PaginationPrefix(*pathURI.Path)
		res, err := client.ListObjectsWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &api.ListObjectsParams{Prefix: &pfx})
		DieOnResponseError(res, err)
		obj_list := res.JSON200.Results
		logCommitsParams := &api.LogCommitsParams{
			Amount: api.PaginationAmountPtr(amount),
		}
		pagination := api.Pagination{HasMore: true}
		for _, obj := range obj_list {
			res, err := client.LogCommitsWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, logCommitsParams)
			DieOnResponseError(res, err)
			pagination = res.JSON200.Pagination
			logCommitsParams.After = api.PaginationAfterPtr(pagination.NextOffset)
			data := struct {
				Commits    []api.Commit
				Object     string
				Pagination *Pagination
			}{
				Commits: res.JSON200.Results,
				Object:  obj.Path,
				Pagination: &Pagination{
					Amount: amount,
				},
			}
			Write(annotateTemplate, data)

		}
	},
}

func init() {
	rootCmd.AddCommand(annotateCmd)
}
