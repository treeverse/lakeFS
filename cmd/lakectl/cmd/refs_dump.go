package cmd

import (
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const metadataDumpTemplate = `{{ .Response | json }}
`

var refsDumpCmd = &cobra.Command{
	Use:               "refs-dump <repository URI>",
	Short:             "Dumps refs (branches, commits, tags) to the underlying object store",
	Hidden:            true,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		repoURI := MustParseRepoURI("repository URI", args[0])
		client := getClient()

		// request refs dump
		resp, err := client.DumpRefsSubmitWithResponse(cmd.Context(), repoURI.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		taskID := resp.JSON201.Id

		// wait for refs dump to complete
		ticker := time.NewTicker(3 * time.Second)
		var statusResp *apigen.DumpRefsStatusResponse
		for range ticker.C {
			statusResp, err = client.DumpRefsStatusWithResponse(cmd.Context(), repoURI.Repository, taskID)
			DieOnErrorOrUnexpectedStatusCode(statusResp, err, http.StatusOK)
			if statusResp.JSON201 == nil {
				DieFmt("Refs dump status failed: %s", statusResp.Status())
			}
			fmt.Printf("Refs dump status: %s\n", statusResp.JSON201.Status)
			if statusResp.JSON201.Status == "completed" || statusResp.JSON201.Status == "failed" {
				break
			}
		}
		if statusResp.JSON201.Refs != nil {
			Write(metadataDumpTemplate, struct {
				Response interface{}
			}{statusResp.JSON201.Refs})
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(refsDumpCmd)
}
