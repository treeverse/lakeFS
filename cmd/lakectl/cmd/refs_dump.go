package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/cenkalti/backoff/v4"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/logging"
)

var refsDumpCmd = &cobra.Command{
	Use:               "refs-dump <repository URI>",
	Short:             "Dumps refs (branches, commits, tags) to the underlying object store",
	Hidden:            true,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		repoURI := MustParseRepoURI("repository URI", args[0])
		client := getClient()
		output := Must(cmd.Flags().GetString("output"))
		checkInterval := Must(cmd.Flags().GetDuration("interval"))
		if checkInterval < minimumCheckInterval {
			DieFmt("Check interval must be at least %s", minimumCheckInterval)
		}

		// request refs dump
		ctx := cmd.Context()
		resp, err := client.DumpRefsSubmitWithResponse(ctx, repoURI.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusAccepted)
		if resp.JSON202 == nil {
			Die("Bad response from server", 1)
		}

		taskID := resp.JSON202.Id
		logging.FromContext(ctx).WithField("task_id", taskID).Debug("Submitted refs dump")

		// wait for refs dump to complete
		dumpStatus, err := backoff.RetryWithData(func() (*apigen.RefsDumpStatus, error) {
			logging.FromContext(ctx).
				WithFields(logging.Fields{"task_id": taskID}).Debug("Checking status of refs dump")

			resp, err := client.DumpRefsStatusWithResponse(ctx, repoURI.Repository, taskID)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				err := fmt.Errorf("dump status %w: %s", ErrRequestFailed, resp.Status())
				return nil, backoff.Permanent(err)
			}
			if !resp.JSON200.Completed {
				return nil, ErrTaskNotCompleted
			}
			return resp.JSON200, nil
		},
			backoff.NewConstantBackOff(checkInterval),
		)
		if err != nil {
			DieErr(err)
		}
		if dumpStatus.Error != nil {
			DieFmt("Refs dump failed: %s", *dumpStatus.Error)
		} else if dumpStatus.Refs == nil {
			Die("Refs dump failed: no refs returned", 1)
		}
		err = printRefs(output, dumpStatus.Refs)
		if err != nil {
			DieErr(err)
		}
	},
}

func printRefs(output string, refs *apigen.RefsDump) error {
	// marshal refs to JSON
	refsJSON, err := json.MarshalIndent(refs, "", "  ")
	if err != nil {
		return err
	}

	// select the output writer
	var w io.Writer
	if output == "" || output == "-" {
		w = os.Stdout
	} else {
		fmt.Println("Writing refs to", output)
		fp, err := os.Create(output)
		if err != nil {
			return err
		}
		w = fp
		defer func() { _ = fp.Close() }()
	}

	// print refs to output
	_, err = fmt.Fprintf(w, "%s\n", refsJSON)
	return err
}

//nolint:gochecknoinits
func init() {
	refsDumpCmd.Flags().StringP("output", "o", "", "output filename (default stdout)")
	refsDumpCmd.Flags().Duration("interval", defaultCheckInterval, "status check interval")
	rootCmd.AddCommand(refsDumpCmd)
}
