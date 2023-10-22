package cmd

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
)

var errInvalidKeyValueFormat = errors.New(`invalid key/value pair - should be separated by "="`)

const fmtErrEmptyMessage = `commit with no message without specifying the "--allow-empty-message" flag`

const (
	dateFlagName              = "epoch-time-seconds"
	messageFlagName           = "message"
	allowEmptyMessageFlagName = "allow-empty-message"
	metaFlagName              = "meta"
	commitCreateTemplate      = `Commit for branch "{{.Branch.Ref}}" completed.

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
		// validate message
		kvPairs, err := getKV(cmd, metaFlagName)
		if err != nil {
			DieErr(err)
		}

		message := Must(cmd.Flags().GetString(messageFlagName))
		emptyMessageBool := Must(cmd.Flags().GetBool(allowEmptyMessageFlagName))
		date := Must(cmd.Flags().GetInt64(dateFlagName))

		if strings.TrimSpace(message) == "" && !emptyMessageBool {
			DieFmt(fmtErrEmptyMessage)
		}

		datePtr := &date
		if date < 0 {
			datePtr = nil
		}

		branchURI := MustParseBranchURI("branch URI", args[0])
		fmt.Println("Branch:", branchURI)

		// do commit
		metadata := apigen.CommitCreation_Metadata{
			AdditionalProperties: kvPairs,
		}
		client := getClient()
		resp, err := client.CommitWithResponse(cmd.Context(), branchURI.Repository, branchURI.Ref, &apigen.CommitParams{}, apigen.CommitJSONRequestBody{
			Message:  message,
			Metadata: &metadata,
			Date:     datePtr,
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

func getKV(cmd *cobra.Command, name string) (map[string]string, error) { //nolint:unparam
	kvList, err := cmd.Flags().GetStringSlice(name)
	if err != nil {
		return nil, err
	}

	kv := make(map[string]string)
	for _, pair := range kvList {
		key, value, found := strings.Cut(pair, "=")
		if !found {
			return nil, errInvalidKeyValueFormat
		}
		kv[key] = value
	}
	return kv, nil
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(commitCmd)

	commitCmd.Flags().Bool(allowEmptyMessageFlagName, false, "allow an empty commit message")
	commitCmd.Flags().StringP(messageFlagName, "m", "", "commit message")

	commitCmd.Flags().Int64(dateFlagName, -1, "create commit with a custom unix epoch date in seconds")
	if err := commitCmd.Flags().MarkHidden(dateFlagName); err != nil {
		DieErr(err)
	}

	commitCmd.Flags().StringSlice(metaFlagName, []string{}, "key value pair in the form of key=value")
}
