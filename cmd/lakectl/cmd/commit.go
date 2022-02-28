package cmd

import (
	"errors"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

var errInvalidKeyValueFormat = errors.New(`invalid key/value pair - should be separated by "="`)

const FmtErrEmptyMessage = `commit with no message without specifying the "--allow-empty-message" flag`

const (
	MessageFlagName           = "message"
	AllowEmptyMessageFlagName = "allow-empty-message"
	MetaFlagName              = "meta"
	CommitCreateTemplate      = `Commit for branch "{{.Branch.Ref}}" completed.

ID: {{.Commit.Id|yellow}}
Message: {{.Commit.Message}}
Timestamp: {{.Commit.CreationDate|date}}
Parents: {{.Commit.Parents|join ", "}}

`
)

var commitCmd = &cobra.Command{
	Use:   "commit <branch uri>",
	Short: "Commit changes on a given branch",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// validate message
		kvPairs, err := getKV(cmd, MetaFlagName)
		if err != nil {
			DieErr(err)
		}

		message := MustString(cmd.Flags().GetString(MessageFlagName))
		emptyMessageBool := MustBool(cmd.Flags().GetBool(AllowEmptyMessageFlagName))

		if strings.TrimSpace(message) == "" && !emptyMessageBool {
			DieFmt(FmtErrEmptyMessage)
		}

		branchURI := MustParseRefURI("branch", args[0])
		Fmt("Branch: %s\n", branchURI.String())

		// do commit
		metadata := api.CommitCreation_Metadata{
			AdditionalProperties: kvPairs,
		}
		client := getClient()
		resp, err := client.CommitWithResponse(cmd.Context(), branchURI.Repository, branchURI.Ref, api.CommitJSONRequestBody{
			Message:  message,
			Metadata: &metadata,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)

		commit := resp.JSON201
		Write(CommitCreateTemplate, struct {
			Branch *uri.URI
			Commit *api.Commit
		}{Branch: branchURI, Commit: commit})
	},
}

func getKV(cmd *cobra.Command, name string) (map[string]string, error) {
	kvList, err := cmd.Flags().GetStringSlice(name)
	if err != nil {
		return nil, err
	}
	const keyValueParts = 2
	kv := make(map[string]string)
	for _, pair := range kvList {
		parts := strings.SplitN(pair, "=", keyValueParts)
		if len(parts) != keyValueParts {
			return nil, errInvalidKeyValueFormat
		}
		kv[parts[0]] = parts[1]
	}
	return kv, nil
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(commitCmd)

	commitCmd.Flags().Bool(AllowEmptyMessageFlagName, false, "allow an empty commit message")
	commitCmd.Flags().StringP(MessageFlagName, "m", "", "commit message")

	commitCmd.Flags().StringSlice(MetaFlagName, []string{}, "key value pair in the form of key=value")
}
