package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/helpers"
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
		pullInterval := Must(cmd.Flags().GetDuration("pull-interval"))
		if pullInterval < minimumPullInterval {
			DieFmt("Pull interval must be at least %s", minimumPullInterval)
		}
		pullExpiry := Must(cmd.Flags().GetDuration("pull-expiry"))

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
				err := fmt.Errorf("dump status %w: %s", helpers.ErrRequestFailed, resp.Status())
				return nil, backoff.Permanent(err)
			}
			if resp.JSON200.Completed {
				return resp.JSON200, nil
			}
			if pullExpiry >= 0 && time.Since(resp.JSON200.UpdateTime) > pullExpiry {
				return nil, backoff.Permanent(ErrTaskNotCompleted)
			}
			return nil, ErrTaskNotCompleted
		}, backoff.WithContext(
			backoff.NewConstantBackOff(pullInterval), ctx),
		)

		switch {
		case err != nil:
			DieErr(err)
		case dumpStatus == nil:
			Die("Refs restore failed: no status returned", 1)
		case dumpStatus.Error != nil:
			DieFmt("Refs dump failed: %s", *dumpStatus.Error)
		case dumpStatus.Refs == nil:
			Die("Refs dump failed: no refs returned", 1)
		}
		if err := printRefs(output, dumpStatus.Refs); err != nil {
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
	refsDumpCmd.Flags().Duration("pull-interval", defaultPullInterval, "pull status check interval")
	refsDumpCmd.Flags().Duration("pull-expiry", defaultPullExpiry, "pull status check expiry")
	rootCmd.AddCommand(refsDumpCmd)
}
