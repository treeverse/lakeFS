package cmd

import (
	"fmt"
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
		u := MustParseRepoURI("Operation requires a valid repository URI. e.g. lakefs://<repo>", args[0])
		fmt.Println("Repository:", u)
		runID := args[1]
		hookRunID := args[2]
		client := getClient()
		ctx := cmd.Context()
		resp, err := client.GetRunHookOutputWithResponse(ctx, u.Repository, runID, hookRunID)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		fmt.Println(string(resp.Body))
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(catHookOutputCmd)
}
