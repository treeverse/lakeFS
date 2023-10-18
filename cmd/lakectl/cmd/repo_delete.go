package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

// repoDeleteCmd represents the delete repo command
// lakectl delete lakefs://myrepo
var repoDeleteCmd = &cobra.Command{
	Use:               "delete <repository uri>",
	Short:             "Delete existing repository",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRepoURI("Operation requires a valid repository URI. e.g. lakefs://<repo>", args[0])
		fmt.Println("Repository:", u)
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete repository: "+u.Repository)
		if err != nil || !confirmation {
			DieFmt("Delete Repository '%s' aborted\n", u.Repository)
		}
		resp, err := clt.DeleteRepositoryWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Printf("Repository '%s' deleted\n", u.Repository)
	},
}

//nolint:gochecknoinits
func init() {
	AssignAutoConfirmFlag(repoDeleteCmd.Flags())

	repoCmd.AddCommand(repoDeleteCmd)
}
