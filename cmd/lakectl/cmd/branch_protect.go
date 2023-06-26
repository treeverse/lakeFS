package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	branchProtectAddCmdArgs    = 2
	branchProtectDeleteCmdArgs = 2
)

var branchProtectCmd = &cobra.Command{
	Use:   "branch-protect",
	Short: "Create and manage branch protection rules",
	Long:  "Define branch protection rules to prevent direct changes. Changes to protected branches can only be done by merging from other branches.",
}

var branchProtectListCmd = &cobra.Command{
	Use:               "list <repo uri>",
	Short:             "List all branch protection rules",
	Example:           "lakectl branch-protect list lakefs://<repository>",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRepoURI("repository", args[0])
		resp, err := client.GetBranchProtectionRulesWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		patterns := make([][]interface{}, len(*resp.JSON200))
		for i, rule := range *resp.JSON200 {
			patterns[i] = []interface{}{rule.Pattern}
		}
		PrintTable(patterns, []interface{}{"Branch Name Pattern"}, &api.Pagination{
			HasMore: false,
			Results: len(patterns),
		}, len(patterns))
	},
}

var branchProtectAddCmd = &cobra.Command{
	Use:               "add <repo uri> <pattern>",
	Short:             "Add a branch protection rule",
	Long:              "Add a branch protection rule for a given branch name pattern",
	Example:           "lakectl branch-protect add lakefs://<repository> 'stable_*'",
	Args:              cobra.ExactArgs(branchProtectAddCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRepoURI("repository", args[0])
		resp, err := client.CreateBranchProtectionRuleWithResponse(cmd.Context(), u.Repository, api.CreateBranchProtectionRuleJSONRequestBody{
			Pattern: args[1],
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Printf("Branch protection rule added to '%s' repository.\n", u.Repository)
	},
}

var branchProtectDeleteCmd = &cobra.Command{
	Use:               "delete <repo uri> <pattern>",
	Short:             "Delete a branch protection rule",
	Long:              "Delete a branch protection rule for a given branch name pattern",
	Example:           "lakectl branch-protect delete lakefs://<repository> stable_*",
	Args:              cobra.ExactArgs(branchProtectDeleteCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRepoURI("repository", args[0])
		resp, err := client.DeleteBranchProtectionRuleWithResponse(cmd.Context(), u.Repository, api.DeleteBranchProtectionRuleJSONRequestBody{
			Pattern: args[1],
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Printf("Branch protection rule deleted from '%s' repository.\n", u.Repository)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(branchProtectCmd)
	branchProtectCmd.AddCommand(branchProtectAddCmd)
	branchProtectCmd.AddCommand(branchProtectListCmd)
	branchProtectCmd.AddCommand(branchProtectDeleteCmd)
}
