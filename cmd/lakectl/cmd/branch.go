package cmd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	ParentNumberFlagName = "parent-number"

	branchRevertCmdArgs = 2
)

// branchCmd represents the branch command
var branchCmd = &cobra.Command{
	Use:   "branch",
	Short: "Create and manage branches within a repository",
	Long:  `Create delete and list branches within a lakeFS repository`,
}

var branchListCmd = &cobra.Command{
	Use:               "list <repository uri>",
	Short:             "List branches in a repository",
	Example:           "lakectl branch list lakefs://<repository>",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		amount := MustInt(cmd.Flags().GetInt("amount"))
		after := MustString(cmd.Flags().GetString("after"))
		u := MustParseRepoURI("repository", args[0])
		client := getClient()
		resp, err := client.ListBranchesWithResponse(cmd.Context(), u.Repository, &api.ListBranchesParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		refs := resp.JSON200.Results
		rows := make([][]interface{}, len(refs))
		for i, row := range refs {
			rows[i] = []interface{}{row.Id, row.CommitId}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Branch", "Commit ID"}, &pagination, amount)
	},
}

var branchCreateCmd = &cobra.Command{
	Use:               "create <branch uri> -s <source ref uri>",
	Short:             "Create a new branch in a repository",
	Example:           "lakectl branch create lakefs://example-repo/new-branch -s lakefs://example-repo/main",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseBranchURI("branch", args[0])
		client := getClient()
		sourceRawURI, _ := cmd.Flags().GetString("source")
		sourceURI, err := uri.ParseWithBaseURI(sourceRawURI, baseURI)
		if err != nil {
			DieFmt("failed to parse source URI: %s", err)
		}
		Fmt("Source ref: %s\n", sourceURI.String())
		if sourceURI.Repository != u.Repository {
			Die("source branch must be in the same repository", 1)
		}

		resp, err := client.CreateBranchWithResponse(cmd.Context(), u.Repository, api.CreateBranchJSONRequestBody{
			Name:   u.Ref,
			Source: sourceURI.Ref,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		Fmt("created branch '%s' %s\n", u.Ref, string(resp.Body))
	},
}

var branchDeleteCmd = &cobra.Command{
	Use:               "delete <branch uri>",
	Short:             "Delete a branch in a repository, along with its uncommitted changes (CAREFUL)",
	Example:           "lakectl branch delete lakefs://example-repo/example-branch",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete branch")
		if err != nil || !confirmation {
			Die("Delete branch aborted", 1)
		}
		client := getClient()
		u := MustParseBranchURI("branch", args[0])
		Fmt("Branch: %s\n", u.String())
		resp, err := client.DeleteBranchWithResponse(cmd.Context(), u.Repository, u.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

// lakectl branch revert lakefs://myrepo/main commitId
var branchRevertCmd = &cobra.Command{
	Use:   "revert <branch uri> <commit ref to revert> [<more commits>...]",
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
		u := MustParseBranchURI("branch", args[0])
		Fmt("Branch: %s\n", u.String())
		hasParentNumber := cmd.Flags().Changed(ParentNumberFlagName)
		parentNumber, _ := cmd.Flags().GetInt(ParentNumberFlagName)
		if hasParentNumber && parentNumber <= 0 {
			Die("parent number must be non-negative, if specified", 1)
		}
		commits := strings.Join(args[1:], " ")
		confirmation, err := Confirm(cmd.Flags(), fmt.Sprintf("Are you sure you want to revert the effect of commits %s", commits))
		if err != nil || !confirmation {
			Die("Revert aborted", 1)
		}
		clt := getClient()
		for i := 1; i < len(args); i++ {
			commitRef := args[i]
			resp, err := clt.RevertBranchWithResponse(cmd.Context(), u.Repository, u.Ref, api.RevertBranchJSONRequestBody{
				ParentNumber: parentNumber,
				Ref:          commitRef,
			})
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
			Fmt("commit %s successfully reverted\n", commitRef)
		}
	},
}

// lakectl branch reset lakefs://myrepo/main --commit commitId --prefix path --object path
var branchResetCmd = &cobra.Command{
	Use:     "reset <branch uri> [--prefix|--object]",
	Example: "lakectl branch reset lakefs://example-repo/example-branch",
	Short:   "Reset uncommitted changes - all of them, or by path",
	Long: `reset changes.  There are four different ways to reset changes:
  1. reset all uncommitted changes - reset lakefs://myrepo/main 
  2. reset uncommitted changes under specific path - reset lakefs://myrepo/main --prefix path
  3. reset uncommitted changes for specific object - reset lakefs://myrepo/main --object path`,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseBranchURI("branch", args[0])
		Fmt("Branch: %s\n", u.String())
		prefix, err := cmd.Flags().GetString("prefix")
		if err != nil {
			DieErr(err)
		}
		object, err := cmd.Flags().GetString("object")
		if err != nil {
			DieErr(err)
		}

		var reset api.ResetCreation
		var confirmationMsg string
		switch {
		case len(prefix) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to reset all uncommitted changes from path: %s", prefix)
			reset = api.ResetCreation{
				Path: &prefix,
				Type: "common_prefix",
			}
		case len(object) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to reset all uncommitted changes for object: %s", object)
			reset = api.ResetCreation{
				Path: &object,
				Type: "object",
			}
		default:
			confirmationMsg = "Are you sure you want to reset all uncommitted changes"
			reset = api.ResetCreation{
				Type: "reset",
			}
		}

		confirmation, err := Confirm(cmd.Flags(), confirmationMsg)
		if err != nil || !confirmation {
			Die("Reset aborted", 1)
			return
		}
		resp, err := clt.ResetBranchWithResponse(cmd.Context(), u.Repository, u.Ref, api.ResetBranchJSONRequestBody(reset))
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

var branchShowCmd = &cobra.Command{
	Use:               "show <branch uri>",
	Example:           "lakectl branch show lakefs://example-repo/example-branch",
	Short:             "Show branch latest commit reference",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseBranchURI("branch", args[0])
		Fmt("Branch: %s\n", u.String())
		resp, err := client.GetBranchWithResponse(cmd.Context(), u.Repository, u.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		branch := resp.JSON200
		Fmt("Commit ID: %s\n", branch.CommitId)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(branchCmd)
	branchCmd.AddCommand(branchCreateCmd)
	branchCmd.AddCommand(branchDeleteCmd)
	branchCmd.AddCommand(branchListCmd)
	branchCmd.AddCommand(branchShowCmd)
	branchCmd.AddCommand(branchResetCmd)
	branchCmd.AddCommand(branchRevertCmd)

	branchListCmd.Flags().Int("amount", defaultAmountArgumentValue, "number of results to return")
	branchListCmd.Flags().String("after", "", "show results after this value (used for pagination)")

	branchCreateCmd.Flags().StringP("source", "s", "", "source branch uri")
	_ = branchCreateCmd.MarkFlagRequired("source")
	_ = branchCreateCmd.RegisterFlagCompletionFunc("source", ValidArgsRepository)

	branchResetCmd.Flags().String("prefix", "", "prefix of the objects to be reset")
	branchResetCmd.Flags().String("object", "", "path to object to be reset")

	branchRevertCmd.Flags().IntP(ParentNumberFlagName, "m", 0, "the parent number (starting from 1) of the mainline. The revert will reverse the change relative to the specified parent.")

	AssignAutoConfirmFlag(branchResetCmd.Flags())
	AssignAutoConfirmFlag(branchRevertCmd.Flags())
	AssignAutoConfirmFlag(branchDeleteCmd.Flags())
}
