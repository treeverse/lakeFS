package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	cherryPickCmdArgs = 2
)

// lakectl cherry-pick lakefs://myrepo/main lakefs://myrepo/some-ref
var cherryPick = &cobra.Command{
	Use:   "cherry-pick <branch uri> <ref uri>",
	Short: "cherry-pick a ref into a branch",
	Long:  "The commit will be applied to the branch as a new commit",
	Example: `lakectl cherry-pick lakefs://myrepo/main lakefs://myrepo/some-ref
	          Cherry picks the commit represented by some-ref into the main branch
              by applying all the changes between the commit and its parent on the destination branch.
             `,
	Args: cobra.ExactArgs(cherryPickCmdArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return validRepositoryToComplete(cmd.Context(), toComplete)
	},
	Run: func(cmd *cobra.Command, args []string) {
		branch := MustParseBranchURI("branch", args[0])
		Fmt("Branch: %s\n", branch.String())

		ref := MustParseRefURI("ref", args[1])

		if branch.Repository != ref.Repository {
			Die("Repository mismatch for destination branch and cherry-pick ref", 1)
		}
		hasParentNumber := cmd.Flags().Changed(ParentNumberFlagName)
		parentNumber, _ := cmd.Flags().GetInt(ParentNumberFlagName)
		if hasParentNumber && parentNumber <= 0 {
			Die("parent number must be non-negative, if specified", 1)
		}

		clt := getClient()
		resp, err := clt.CherryPickWithResponse(cmd.Context(), branch.Repository, branch.Ref, api.CherryPickJSONRequestBody{
			Ref:          ref.Ref,
			ParentNumber: &parentNumber, // TODO: handle default value 0
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)

		resp2, err := clt.GetCommitWithResponse(cmd.Context(), branch.Repository, branch.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp2, err, http.StatusOK)
		Write(commitCreateTemplate, struct {
			Branch *uri.URI
			Commit *api.Commit
		}{Branch: branch, Commit: resp2.JSON200})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(cherryPick)

	cherryPick.Flags().IntP(ParentNumberFlagName, "m", 0, "the parent number (starting from 1) of the mainline. The cherry-pick will apply the change relative to the specified parent.")
}
