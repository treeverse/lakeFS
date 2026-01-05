package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var gcCheckCommitsTemplate = `GC commits task {{.TaskId|yellow}}: {{if .Completed -}} Completed {{- else -}} Running. {{- end}}

{{- with .Error}}{{"Failed:" | red | bold}}{{. | bold}}{{end}}
{{with .Result -}}
Commits location: {{.GcCommitsLocation | blue}}
{{with .GcCommitsPresignedUrl}}  Presigned URL:  {{ blue . }}{{end}}
  Run ID:           {{.RunId}}
{{end}}

Last updated {{.UpdateTime}}.
`

var gcCheckCommitsCmd = &cobra.Command{
	Use:               "check-async <repository URI>",
	Short:             "Check status of (async) PrepareGarbageCollectionCommits",
	Example:           "lakectl gc prepare-async --id <ID>" + myRepoExample,
	Args:              cobra.ExactArgs(1),
	Hidden:            true,
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("repository URI", args[0])
		client := getClient()
		id := Must(cmd.Flags().GetString("id"))
		resp, err := client.PrepareGarbageCollectionCommitsStatusWithResponse(cmd.Context(), u.Repository, &apigen.PrepareGarbageCollectionCommitsStatusParams{Id: id})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)

		Write(gcCheckCommitsTemplate, resp.JSON200)
	},
}

//nolint:gochecknoinits
func init() {
	gcCheckCommitsCmd.Flags().String("id", "", "ID returned from \"gc prepare-async\"")
	gcCmd.AddCommand(gcCheckCommitsCmd)
}
