package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
)

const catHookOutputRequiredArgs = 3

var catHookOutputCmd = &cobra.Command{
	Use:               "cat-hook-output",
	Short:             "Cat actions hook output",
	Hidden:            true,
	Example:           "lakectl cat-hook-output lakefs://<repository> <run_id> <run_hook_id>",
	Args:              cobra.ExactArgs(catHookOutputRequiredArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("repository", args[0])
		Fmt("Repository: %s\n", u.String())
		runID := args[1]
		hookRunID := args[2]
		client := getClient()
		ctx := cmd.Context()
		resp, err := client.GetRunHookOutputWithResponse(ctx, u.Repository, runID, hookRunID)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		Fmt("%s\n", string(resp.Body))
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(catHookOutputCmd)
}
