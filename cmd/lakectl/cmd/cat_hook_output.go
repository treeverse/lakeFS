package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

const catHookOutputRequiredArgs = 3

var catHookOutputCmd = &cobra.Command{
	Use:               "cat-hook-output <repository uri> <run_id> <hook_id>",
	Short:             "Cat actions hook output",
	Hidden:            true,
	Example:           fmt.Sprintf("lakectl cat-hook-output %s %s my_hook_name", myRepoExample, runIDExample),
	Args:              cobra.ExactArgs(catHookOutputRequiredArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("Repository", args[0])
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
