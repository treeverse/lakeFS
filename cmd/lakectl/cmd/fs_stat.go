package cmd

import (
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var fsStatCmd = &cobra.Command{
	Use:               "stat <path uri>",
	Short:             "View object metadata",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path", args[0])
		preSign := Must(cmd.Flags().GetBool("pre-sign"))
		client := getClient()
		resp, err := client.StatObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &apigen.StatObjectParams{
			Path:         *pathURI.Path,
			Presign:      swag.Bool(preSign),
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
	fsStatCmd.Flags().Bool("pre-sign", false, "Request pre-sign for physical address")

	fsCmd.AddCommand(fsStatCmd)
}
