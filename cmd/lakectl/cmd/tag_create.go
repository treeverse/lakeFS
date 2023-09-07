package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const tagCreateRequiredArgs = 2

var tagCreateCmd = &cobra.Command{
	Use:     "create <tag uri> <commit uri>",
	Short:   "Create a new tag in a repository",
	Example: "lakectl tag create lakefs://example-repo/example-tag lakefs://example-repo/2397cc9a9d04c20a4e5739b42c1dd3d8ba655c0b3a3b974850895a13d8bf9917",
	Args:    cobra.ExactArgs(tagCreateRequiredArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) >= tagCreateRequiredArgs {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return validRepositoryToComplete(cmd.Context(), toComplete)
	},
	Run: func(cmd *cobra.Command, args []string) {
		tagURI := MustParseRefURI("tag uri", args[0])
		commitURI := MustParseRefURI("commit uri", args[1])
		fmt.Println("Tag ref:", tagURI)

		client := getClient()
		ctx := cmd.Context()
		force := Must(cmd.Flags().GetBool("force"))

		if tagURI.Repository != commitURI.Repository {
			Die("both references must belong to the same repository", 1)
		}

		if force {
			// checking the validity of the commitRef before deleting the old one
			res, err := client.GetCommitWithResponse(ctx, tagURI.Repository, commitURI.Ref)
			DieOnErrorOrUnexpectedStatusCode(res, err, http.StatusOK)
			if res.JSON200 == nil {
				Die("Bad response from server", 1)
			}

			resp, err := client.DeleteTagWithResponse(ctx, tagURI.Repository, tagURI.Ref)
			if err != nil && (resp == nil || resp.JSON404 == nil) {
				DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
			}
		}

		resp, err := client.CreateTagWithResponse(ctx, tagURI.Repository, apigen.CreateTagJSONRequestBody{
			Id:  tagURI.Ref,
			Ref: commitURI.Ref,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		commitID := *resp.JSON201
		fmt.Printf("Created tag '%s' (%s)\n", tagURI.Ref, commitID)
	},
}

//nolint:gochecknoinits
func init() {
	tagCreateCmd.Flags().BoolP("force", "f", false, "override the tag if it exists")
	tagCmd.AddCommand(tagCreateCmd)
}
