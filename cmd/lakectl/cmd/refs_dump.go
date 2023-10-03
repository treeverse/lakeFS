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
		var dumpStatus *apigen.RefsDumpStatus
		for range ticker.C {
			fmt.Println("Checking status of refs dump")
			resp, err := client.DumpRefsStatusWithResponse(cmd.Context(), repoURI.Repository, taskID)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				DieFmt("Refs dump status failed: %s", resp.Status())
			}
			dumpStatus = resp.JSON200
			if dumpStatus.Completed {
				break
			}
		}
		if dumpStatus.Error != nil {
			DieFmt("Refs dump failed: %s", *dumpStatus.Error)
		} else if dumpStatus.Refs == nil {
			Die("Refs dump failed: no refs returned", 1)
		} else {
			Write(metadataDumpTemplate, struct {
				Response interface{}
			}{Response: dumpStatus.Refs})
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(refsDumpCmd)
}
