package cmd

import (
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var tagDeleteCmd = &cobra.Command{
	Use:               "delete <tag URI>",
	Short:             "Delete a tag from a repository",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete tag")
		if err != nil || !confirmation {
			Die("Delete tag aborted", 1)
		}
		client := getClient()
		u := MustParseRefURI("tag URI", args[0])

		ctx := cmd.Context()
		ignore := Must(cmd.Flags().GetBool(ignoreFlagName))
		resp, err := client.DeleteTagWithResponse(ctx, u.Repository, u.Ref, &apigen.DeleteTagParams{Force: swag.Bool(ignore)})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

//nolint:gochecknoinits
func init() {
	AssignAutoConfirmFlag(tagDeleteCmd.Flags())
	withIgnoreFlag(tagDeleteCmd)
	tagCmd.AddCommand(tagDeleteCmd)
}
