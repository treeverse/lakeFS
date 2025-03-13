package cmd

import (
	"io"
	"net/http"
	"os"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var fsCatCmd = &cobra.Command{
	Use:               "cat <path URI>",
	Short:             "Dump content of object to stdout",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path URI", args[0])
		client := getClient()
		preSignMode := getPresignMode(cmd, client, pathURI.Repository)

		var err error
		var body io.ReadCloser
		var resp *http.Response
		resp, err = client.GetObject(cmd.Context(), pathURI.Repository, pathURI.Ref, &apigen.GetObjectParams{
			Path:    *pathURI.Path,
			Presign: swag.Bool(preSignMode.Enabled),
		})
		DieOnHTTPError(resp)
		body = resp.Body
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
	withPresignFlag(fsCatCmd)
	fsCmd.AddCommand(fsCatCmd)
}
