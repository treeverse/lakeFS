package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
)

var errInvalidKeyValueFormat = errors.New(`invalid key/value pair - should be separated by "="`)

const (
	dateFlagName         = "epoch-time-seconds"
	allowEmptyCommit     = "allow-empty-commit"
	commitCreateTemplate = `Commit for branch "{{.Branch.Ref}}" completed.

ID: {{.Commit.Id|yellow}}
Message: {{.Commit.Message}}
Timestamp: {{.Commit.CreationDate|date}}
Parents: {{.Commit.Parents|join ", "}}

`
)

var commitCmd = &cobra.Command{
	Use:               "commit <branch URI>",
	Short:             "Commit changes on a given branch",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		message, kvPairs := getCommitFlags(cmd)
		date := Must(cmd.Flags().GetInt64(dateFlagName))
		emptyCommitBool := Must(cmd.Flags().GetBool(allowEmptyCommit))
		datePtr := &date
		if date < 0 {
			datePtr = nil
		}

		branchURI := MustParseBranchURI("branch URI", args[0])
		fmt.Println("Branch:", branchURI)

		ctx := cmd.Context()
		client := getClient()

		configResp, err := client.GetConfigWithResponse(ctx)
		DieOnErrorOrUnexpectedStatusCode(configResp, err, http.StatusOK)
		if configResp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		body := apigen.CommitJSONRequestBody{
			Message: message,
			Metadata: &apigen.CommitCreation_Metadata{
				AdditionalProperties: kvPairs,
			},
			Date:       datePtr,
			AllowEmpty: &emptyCommitBool,
		}
		var commit *apigen.Commit
		isAsync := false
		if configResp.JSON200.CapabilitiesConfig != nil {
			isAsync = swag.BoolValue(configResp.JSON200.CapabilitiesConfig.AsyncOps)
		}
		// run asynchronous commit first
		if isAsync {
			sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			startResp, err := client.CommitAsyncWithResponse(sigCtx, branchURI.Repository, branchURI.Ref, &apigen.CommitAsyncParams{}, apigen.CommitAsyncJSONRequestBody(body))
			DieOnErrorOrUnexpectedStatusCode(startResp, err, http.StatusAccepted)
			if startResp.JSON202 == nil {
				Die("Bad response from server", 1)
			}

			taskID := startResp.JSON202.Id
			err = pollAsyncOperationStatus(sigCtx, taskID, "commit", func() (*apigen.AsyncTaskStatus, error) {
				resp, err := client.CommitAsyncStatusWithResponse(sigCtx, branchURI.Repository, branchURI.Ref, taskID)
				if err != nil {
					return nil, err
				}
				if resp.JSON200 == nil {
					Die("Bad response from server", 1)
				}
				commit = resp.JSON200.Result
				return &resp.JSON200.AsyncTaskStatus, nil
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					DieFmt("Commit polling canceled. Async task ID: %s", taskID)
				}
				DieErr(err)
			}
		} else { // Regular commit
			resp, err := client.CommitWithResponse(ctx, branchURI.Repository, branchURI.Ref, &apigen.CommitParams{}, body)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
			if resp.JSON201 == nil {
				Die("Bad response from server", 1)
			}

			commit = resp.JSON201
		}

		Write(commitCreateTemplate, struct {
			Branch *uri.URI
			Commit *apigen.Commit
		}{Branch: branchURI, Commit: commit})
	},
}

//nolint:gochecknoinits
func init() {
	commitCmd.Flags().Int64(dateFlagName, -1, "create commit with a custom unix epoch date in seconds")
	commitCmd.Flags().Bool(allowEmptyCommit, false, "allow a commit with no changes")
	if err := commitCmd.Flags().MarkHidden(dateFlagName); err != nil {
		DieErr(err)
	}
	withCommitFlags(commitCmd, false)
	rootCmd.AddCommand(commitCmd)
}
