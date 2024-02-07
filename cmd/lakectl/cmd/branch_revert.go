package cmd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	branchRevertCmdArgs  = 2
	ParentNumberFlagName = "parent-number"
)

// lakectl branch revert lakefs://myrepo/main commitId
var branchRevertCmd = &cobra.Command{
	Use:   "revert <branch URI> <commit ref to revert> [<more commits>...]",
	Short: "Given a commit, record a new commit to reverse the effect of this commit",
	Long:  "The commits will be reverted in left-to-right order",
	Example: `lakectl branch revert lakefs://example-repo/example-branch commitA
	          Revert the changes done by commitA in example-branch
		      branch revert lakefs://example-repo/example-branch HEAD~1 HEAD~2 HEAD~3
		      Revert the changes done by the second last commit to the fourth last commit in example-branch`,
	Args: cobra.MinimumNArgs(branchRevertCmdArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return validRepositoryToComplete(cmd.Context(), toComplete)
	},
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseBranchURI("branch URI", args[0])
		fmt.Println("Branch:", u)
		hasParentNumber := cmd.Flags().Changed(ParentNumberFlagName)
		parentNumber := Must(cmd.Flags().GetInt(ParentNumberFlagName))
		allowEmptyRevert := Must(cmd.Flags().GetBool(allowEmptyCommit))
		if hasParentNumber && parentNumber <= 0 {
			Die("parent number must be number greater than 0, if specified", 1)
		}
		commits := strings.Join(args[1:], " ")
		confirmation, err := Confirm(cmd.Flags(), fmt.Sprintf("Are you sure you want to revert the effect of commits %s", commits))
		if err != nil || !confirmation {
			Die("Revert aborted", 1)
		}
		clt := getClient()
		for i := 1; i < len(args); i++ {
			commitRef := args[i]
			resp, err := clt.RevertBranchWithResponse(cmd.Context(), u.Repository, u.Ref, apigen.RevertBranchJSONRequestBody{
				ParentNumber: parentNumber,
				Ref:          commitRef,
				AllowEmpty:   &allowEmptyRevert,
			})
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
			fmt.Printf("commit %s successfully reverted\n", commitRef)
		}
	},
}

//nolint:gochecknoinits
func init() {
	AssignAutoConfirmFlag(branchRevertCmd.Flags())

	branchRevertCmd.Flags().IntP(ParentNumberFlagName, "m", 0, "the parent number (starting from 1) of the mainline. The revert will reverse the change relative to the specified parent.")
	branchRevertCmd.Flags().Bool(allowEmptyCommit, false, "allow empty commit (revert without changes)")

	branchCmd.AddCommand(branchRevertCmd)
}
