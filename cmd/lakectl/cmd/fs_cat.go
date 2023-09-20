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
	Use:               "cat <path uri>",
	Short:             "Dump content of object to stdout",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path", args[0])
		flagSet := cmd.Flags()
		preSignMode := Must(flagSet.GetBool("pre-sign"))

		var err error
		var body io.ReadCloser
		client := getClient()
		var resp *http.Response
		resp, err = client.GetObject(cmd.Context(), pathURI.Repository, pathURI.Ref, &apigen.GetObjectParams{
			Path:    *pathURI.Path,
			Presign: swag.Bool(preSignMode),
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
	fsCatCmd.Flags().Bool("pre-sign", false, "Use pre-sign link to access the data")

	fsCmd.AddCommand(fsCatCmd)
}
