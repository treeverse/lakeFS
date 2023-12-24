package cmd

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
)

var errInvalidKeyValueFormat = errors.New(`invalid key/value pair - should be separated by "="`)

const (
	dateFlagName         = "epoch-time-seconds"
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
		datePtr := &date
		if date < 0 {
			datePtr = nil
		}

		branchURI := MustParseBranchURI("branch URI", args[0])
		fmt.Println("Branch:", branchURI)

		ignore := Must(cmd.Flags().GetBool("ignore"))

		// do commit
		metadata := apigen.CommitCreation_Metadata{
			AdditionalProperties: kvPairs,
		}
		client := getClient()
		resp, err := client.CommitWithResponse(cmd.Context(), branchURI.Repository, branchURI.Ref, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message:  message,
			Metadata: &metadata,
			Date:     datePtr,
			Force:    swag.Bool(ignore),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		commit := resp.JSON201
		Write(commitCreateTemplate, struct {
			Branch *uri.URI
			Commit *apigen.Commit
		}{Branch: branchURI, Commit: commit})
	},
}

//nolint:gochecknoinits
func init() {
	commitCmd.Flags().Int64(dateFlagName, -1, "create commit with a custom unix epoch date in seconds")
	if err := commitCmd.Flags().MarkHidden(dateFlagName); err != nil {
		DieErr(err)
	}
	withCommitFlags(commitCmd, false)
	rootCmd.AddCommand(commitCmd)
}
