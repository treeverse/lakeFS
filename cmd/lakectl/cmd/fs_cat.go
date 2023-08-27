package cmd

import (
	"io"
	"net/http"
	"os"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
)

var fsCatCmd = &cobra.Command{
	Use:               "cat <path uri>",
	Short:             "Dump content of object to stdout",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path", args[0])
		flagSet := cmd.Flags()
		direct := Must(flagSet.GetBool("direct"))
		preSignMode := Must(flagSet.GetBool("pre-sign"))
		transport := transportMethodFromFlags(direct, preSignMode)

		var err error
		var body io.ReadCloser
		client := getClient()
		if transport == transportMethodDirect {
			_, body, err = helpers.ClientDownload(cmd.Context(), client, pathURI.Repository, pathURI.Ref, *pathURI.Path)
		} else {
			preSign := swag.Bool(transport == transportMethodPreSign)
			var resp *http.Response
			resp, err = client.GetObject(cmd.Context(), pathURI.Repository, pathURI.Ref, &api.GetObjectParams{
				Path:    *pathURI.Path,
				Presign: preSign,
			})
			DieOnHTTPError(resp)
			body = resp.Body
		}
		if err != nil {
			DieErr(err)
		}

		defer func() {
			if err := body.Close(); err != nil {
				DieErr(err)
			}
		}()
		_, err = io.Copy(os.Stdout, body)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	fsCatCmd.Flags().BoolP("direct", "d", false, "read directly from backing store (faster but requires more credentials)")
	err := fsCatCmd.Flags().MarkDeprecated("direct", "use --pre-sign instead")
	if err != nil {
		DieErr(err)
	}
	fsCatCmd.Flags().Bool("pre-sign", false, "Use pre-sign link to access the data")

	fsCmd.AddCommand(fsCatCmd)
}
