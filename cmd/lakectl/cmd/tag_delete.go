package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var tagDeleteCmd = &cobra.Command{
	Use:               "delete <tag uri>",
	Short:             "Delete a tag from a repository",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete tag")
		if err != nil || !confirmation {
			Die("Delete tag aborted", 1)
		}
		client := getClient()
		u := MustParseRefURI("Operation requires a valid tag URI. e.g. lakefs://<repo>/<tag>", args[0])
		fmt.Println("Tag ref:", u)

		ctx := cmd.Context()
		resp, err := client.DeleteTagWithResponse(ctx, u.Repository, u.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

//nolint:gochecknoinits
func init() {
	tagCmd.AddCommand(tagDeleteCmd)
}
