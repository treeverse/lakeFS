package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

var commitCreateTemplate = `Commit for branch "{{.Branch.Ref}}" completed.

ID: {{.Commit.ID|yellow}}
Message: {{.Commit.Message}}
Timestamp: {{.Commit.CreationDate|date}}
Parents: {{.Commit.Parents|join ", "}}

`
var (
	errInvalidKeyValueFormat = fmt.Errorf("invalid key/value pair - should be separated by \"=\"")
)

var commitCmd = &cobra.Command{
	Use:   "commit <branch uri>",
	Short: "commit changes on a given branch",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		// validate message
		kvPairs, err := getKV(cmd, "meta")
		if err != nil {
			DieErr(err)
		}
		message, err := cmd.Flags().GetString("message")
		if err != nil {
			DieErr(err)
		}
		branchURI := uri.Must(uri.Parse(args[0]))

		// do commit
		client := getClient()
		commit, err := client.Commit(context.Background(), branchURI.Repository, branchURI.Ref, message, kvPairs)
		if err != nil {
			DieErr(err)
		}

		Write(commitCreateTemplate, struct {
			Branch *uri.URI
			Commit *models.Commit
		}{branchURI, commit})
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

	commitCmd.Flags().StringP("message", "m", "", "commit message")
	_ = commitCmd.MarkFlagRequired("message")

	commitCmd.Flags().StringSlice("meta", []string{}, "key value pair in the form of key=value")
}
