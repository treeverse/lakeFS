package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
)

const gcDeleteConfigCmdArgs = 1

var gcDeleteConfigCmd = &cobra.Command{
	Use:               "delete-config <repository URI>",
	Short:             "Deletes the garbage collection policy for the repository",
	Example:           "lakectl gc delete-config " + myRepoExample,
	Args:              cobra.ExactArgs(gcDeleteConfigCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("repository URI", args[0])
		client := getClient()
		resp, err := client.DeleteGCRulesWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

//nolint:gochecknoinits
func init() {
	gcCmd.AddCommand(gcDeleteConfigCmd)
}
