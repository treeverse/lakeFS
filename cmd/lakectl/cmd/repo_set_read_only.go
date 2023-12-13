package cmd

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
)

const repoSetReadOnlyCmdArgs = 2

// repoSetReadOnlyCmd represents the set repo read-only command
var repoSetReadOnlyCmd = &cobra.Command{
	Use:               "set-read-only <repository URI> <read-only>",
	Short:             "Set read-only value for repository",
	Example:           "lakectl repo set-read-only " + myRepoExample + " true",
	Args:              cobra.ExactArgs(repoSetReadOnlyCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		fmt.Println("Repository:", u)
		readOnly, err := strconv.ParseBool(args[1])
		if err != nil {
			DieErr(err)
		}
		resp, err := clt.SetRepositoryReadOnly(cmd.Context(), u.Repository, readOnly)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		fmt.Printf("Set repository '%s' read-only to be: %v \n", u.Repository, readOnly)
	},
}

//nolint:gochecknoinits
func init() {
	repoCmd.AddCommand(repoSetReadOnlyCmd)
}
