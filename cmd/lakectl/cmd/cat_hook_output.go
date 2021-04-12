package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/uri"
)

const catHookOutputRequiredArgs = 3

var catHookOutputCmd = &cobra.Command{
	Use:     "cat-hook-output",
	Short:   "Cat actions hook output",
	Hidden:  true,
	Example: "lakectl cat-hook-output lakefs://<repository> <run_id> <run_hook_id>",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(catHookOutputRequiredArgs),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseURI(args[0])
		runID := args[1]
		hookRunID := args[2]
		client := getClient()
		ctx := cmd.Context()
		resp, err := client.GetRunHookOutputWithResponse(ctx, u.Repository, runID, hookRunID)
		DieOnResponseError(resp, err)
		Fmt("%s\n", string(resp.Body))
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(catHookOutputCmd)
}
