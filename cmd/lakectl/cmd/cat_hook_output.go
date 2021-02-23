package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
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
		u := uri.Must(uri.Parse(args[0]))
		runID := args[1]
		hookRunID := args[2]
		client := getClient()
		ctx := context.Background()
		err := client.GetRunHookOutput(ctx, u.Repository, runID, hookRunID, os.Stdout)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(catHookOutputCmd)
}
