package cmd

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

// bisectStartCmd represents the start command
var bisectStartCmd = &cobra.Command{
	Use:   "start <bad ref> <good ref>",
	Short: "Start a bisect session",
	Args:  cobra.ExactArgs(bisectStartCmdArgs),
	Run: func(cmd *cobra.Command, args []string) {
		badURI := MustParseRefURI("Operation requires a valid (bad) reference URI. e.g. lakefs://<repo>/<ref>", args[0])
		goodURI := MustParseRefURI("Operation requires a valid (good) reference URI. e.g. lakefs://<repo>/<ref>", args[1])
		if goodURI.Repository != badURI.Repository {
			Die("Two references doesn't use the same repository", 1)
		}
		repository := goodURI.Repository

		// resolve repository and references
		client := getClient()
		ctx := cmd.Context()
		// check repository exists
		repoResponse, err := client.GetRepositoryWithResponse(ctx, repository)
		DieOnErrorOrUnexpectedStatusCode(repoResponse, err, http.StatusOK)
		if repoResponse.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		state := &Bisect{
			Created:    time.Now().UTC(),
			Repository: repoResponse.JSON200.Id,
			BadCommit:  resolveCommitOrDie(ctx, client, badURI.Repository, badURI.Ref),
			GoodCommit: resolveCommitOrDie(ctx, client, goodURI.Repository, goodURI.Ref),
		}
		// resolve commits
		if err := state.Update(ctx, client); err != nil {
			DieErr(err)
		}
		if err := state.Save(); err != nil {
			DieErr(err)
		}
		state.PrintStatus()
	},
}

//nolint:gochecknoinits
func init() {
	bisectCmd.AddCommand(bisectStartCmd)
}
