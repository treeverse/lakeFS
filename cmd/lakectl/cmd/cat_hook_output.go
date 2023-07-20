package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
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
		u := utils.MustParseRepoURI("repository", args[0])
		utils.Fmt("Repository: %s\n", u.String())
		runID := args[1]
		hookRunID := args[2]
		client := getClient()
		ctx := cmd.Context()
		resp, err := client.GetRunHookOutputWithResponse(ctx, u.Repository, runID, hookRunID)
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		utils.Fmt("%s\n", string(resp.Body))
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(catHookOutputCmd)
}
