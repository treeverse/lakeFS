package cmd

import (
	"fmt"
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var fsPresignCmd = &cobra.Command{
	Use:               "presign <path URI>",
	Short:             "return a pre-signed URL for reading the specified object",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path URI", args[0])
		client := getClient()
		preSignMode := getServerPreSignMode(cmd.Context(), client)
		if !preSignMode.Enabled {
			Die("Pre-signed URL support is currently disabled for this lakeFS server", 1)
		}

		resp, err := client.StatObjectWithResponse(cmd.Context(), pathURI.Repository, pathURI.Ref, &apigen.StatObjectParams{
			Path:         *pathURI.Path,
			Presign:      swag.Bool(preSignMode.Enabled),
			UserMetadata: swag.Bool(true),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		fmt.Printf("%s\n", resp.JSON200.PhysicalAddress)
	},
}

//nolint:gochecknoinits
func init() {
	fsCmd.AddCommand(fsPresignCmd)
}
