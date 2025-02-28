package cmd

import (
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var fsStatCmd = &cobra.Command{
	Use:               "stat <path URI>",
	Short:             "View object metadata",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path URI", args[0])
		client := getClient()
		preSignMode := getPresignMode(cmd, client, pathURI.Repository)

		resp, err := client.StatObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &apigen.StatObjectParams{
			Path:         *pathURI.Path,
			Presign:      swag.Bool(preSignMode.Enabled),
			UserMetadata: swag.Bool(true),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		Write(fsStatTemplate, resp.JSON200)
	},
}

const fsStatTemplate = `Path: {{.Path | yellow }}
Modified Time: {{.Mtime|date}}
Size: {{ .SizeBytes }} bytes
Human Size: {{ .SizeBytes|human_bytes }}
Physical Address: {{ .PhysicalAddress }}
{{- if .PhysicalAddressExpiry }}
Physical Address Expires: {{ .PhysicalAddressExpiry|date }}{{end}}
Checksum: {{ .Checksum }}
Content-Type: {{ .ContentType }}{{ if and $.Metadata $.Metadata.AdditionalProperties }}
Metadata:
	{{ range $key, $value := .Metadata.AdditionalProperties }}
	{{ $key | printf "%-18s" }} = {{ $value }}
	{{- end }}
{{- end }}
`

//nolint:gochecknoinits
func init() {
	withPresignFlag(fsStatCmd)
	fsCmd.AddCommand(fsStatCmd)
}
