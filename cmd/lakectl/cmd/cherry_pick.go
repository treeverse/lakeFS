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
	Use:   "cherry-pick <source ref> <branch ref>",
	Short: "Cherry-Pick a ref into a branch",
	Long: `The commit will be applied to the branch as a new commit.
Cherry picks the commit represented by some-ref into the main branch
by applying all the changes between the commit and its parent on the destination branch.`,
	Example: `lakectl cherry-pick lakefs://myrepo/some-ref lakefs://myrepo/main
`,

	Args: cobra.ExactArgs(cherryPickCmdArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return validRepositoryToComplete(cmd.Context(), toComplete)
	},
	Run: func(cmd *cobra.Command, args []string) {
		ref := MustParseRefURI("ref", args[0])
		branch := MustParseBranchURI("branch", args[1])
		Fmt("Branch: %s\n", branch.String())

		if branch.Repository != ref.Repository {
			Die("Repository mismatch for destination branch and cherry-pick ref", 1)
		}
		hasParentNumber := cmd.Flags().Changed(ParentNumberFlagName)
		parentNumber, _ := cmd.Flags().GetInt(ParentNumberFlagName)
		if hasParentNumber {
			if parentNumber <= 0 {
				Die("parent number must be positive, if specified", 1)
			}
		} else {
			parentNumber = 1
		}

		clt := getClient()
		resp, err := clt.CherryPickWithResponse(cmd.Context(), branch.Repository, branch.Ref, api.CherryPickJSONRequestBody{
			Ref:          ref.Ref,
			ParentNumber: &parentNumber,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)

		Write(commitCreateTemplate, struct {
			Branch *uri.URI
			Commit *api.Commit
		}{Branch: branch, Commit: resp.JSON201})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(cherryPick)

	cherryPick.Flags().IntP(ParentNumberFlagName, "m", 0, "the parent number (starting from 1) of the cherry-picked commit. The cherry-pick will apply the change relative to the specified parent.")
}
