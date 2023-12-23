package cmd

import (
	"fmt"
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	cherryPickCmdArgs = 2
)

var cherryPick = &cobra.Command{
	Use:     "cherry-pick <commit URI> <branch>",
	Short:   "Apply the changes introduced by an existing commit",
	Long:    `Apply the changes from the given commit to the tip of the branch. The changes will be added as a new commit.`,
	Example: "lakectl cherry-pick " + myRepoExample + "/" + myDigestExample + " " + myRepoExample + "/" + myBranchExample,

	Args: cobra.ExactArgs(cherryPickCmdArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return validRepositoryToComplete(cmd.Context(), toComplete)
	},
	Run: func(cmd *cobra.Command, args []string) {
		ref := MustParseRefURI("commit URI", args[0])
		branch := MustParseBranchURI("branch URI", args[1])
		fmt.Println("Branch:", branch)
		force := Must(cmd.Flags().GetBool("force"))

		if branch.Repository != ref.Repository {
			Die("Repository mismatch for destination branch and cherry-pick ref", 1)
		}
		hasParentNumber := cmd.Flags().Changed(ParentNumberFlagName)
		parentNumber := Must(cmd.Flags().GetInt(ParentNumberFlagName))
		if hasParentNumber {
			if parentNumber <= 0 {
				Die("parent number must be positive, if specified", 1)
			}
		} else {
			parentNumber = 1
		}

		clt := getClient()
		resp, err := clt.CherryPickWithResponse(cmd.Context(), branch.Repository, branch.Ref, apigen.CherryPickJSONRequestBody{
			Ref:          ref.Ref,
			ParentNumber: &parentNumber,
			Force:        swag.Bool(force),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)

		Write(commitCreateTemplate, struct {
			Branch *uri.URI
			Commit *apigen.Commit
		}{Branch: branch, Commit: resp.JSON201})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(cherryPick)

	cherryPick.Flags().IntP(ParentNumberFlagName, "m", 0, "the parent number (starting from 1) of the cherry-picked commit. The cherry-pick will apply the change relative to the specified parent.")
	cherryPick.Flags().BoolP("force", "f", false, "ignore read-only protection on the repository")
}
