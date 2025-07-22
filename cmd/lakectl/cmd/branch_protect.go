package cmd

import (
	"net/http"
	"slices"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
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
	Use:               "list <repository URI>",
	Short:             "List all branch protection rules",
	Example:           "lakectl branch-protect list " + myRepoExample,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		resp, err := client.GetBranchProtectionRulesWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		rows := make([][]interface{}, len(*resp.JSON200))
		for i, rule := range *resp.JSON200 {
			blockedActions := "staging_write, commit" // default if not specified
			if rule.BlockedActions != nil && len(*rule.BlockedActions) > 0 {
				blockedActions = ""
				for j, action := range *rule.BlockedActions {
					if j > 0 {
						blockedActions += ", "
					}
					blockedActions += action
				}
			}
			rows[i] = []interface{}{rule.Pattern, blockedActions}
		}
		PrintTable(rows, []interface{}{"Branch Name Pattern", "Blocked Actions"}, &apigen.Pagination{
			HasMore: false,
			Results: len(rows),
		}, len(rows))
	},
}

var branchProtectAddCmd = &cobra.Command{
	Use:   "add <repository URI> <pattern>",
	Short: "Add a branch protection rule",
	Long: `Add a branch protection rule for a given branch name pattern.

You can configure which operations to block using the flags below.
By default, staging writes and commits are blocked, but branch deletion is allowed.`,
	Example: `lakectl branch-protect add ` + myRepoExample + ` 'stable_*'
lakectl branch-protect add ` + myRepoExample + ` 'main' --block-deletion
lakectl branch-protect add ` + myRepoExample + ` 'dev_*' --no-block-staging-writes --block-deletion`,
	Args:              cobra.ExactArgs(branchProtectAddCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		resp, err := client.GetBranchProtectionRulesWithResponse(cmd.Context(), u.Repository)

		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		rules := *resp.JSON200

		// Build blocked actions based on flags
		var blockedActions []string

		// Check for explicit disable flags first
		noBlockStagingWrites, _ := cmd.Flags().GetBool("no-block-staging-writes")
		noBlockCommits, _ := cmd.Flags().GetBool("no-block-commits")

		// Get positive flags with defaults
		blockStagingWrites, _ := cmd.Flags().GetBool("block-staging-writes")
		blockCommits, _ := cmd.Flags().GetBool("block-commits")
		blockDeletion, _ := cmd.Flags().GetBool("block-deletion")

		// Apply logic: default true unless explicitly disabled
		if blockStagingWrites && !noBlockStagingWrites {
			blockedActions = append(blockedActions, "staging_write")
		}
		if blockCommits && !noBlockCommits {
			blockedActions = append(blockedActions, "commit")
		}
		if blockDeletion {
			blockedActions = append(blockedActions, "delete")
		}

		// Create the rule with blocked actions
		rule := apigen.BranchProtectionRule{
			Pattern: args[1],
		}
		if len(blockedActions) > 0 {
			rule.BlockedActions = &blockedActions
		}

		rules = append(rules, rule)
		params := &apigen.SetBranchProtectionRulesParams{}
		etag := swag.String(resp.HTTPResponse.Header.Get("ETag"))
		if etag != nil && *etag != "" {
			params.IfMatch = etag
		}
		setResp, err := client.SetBranchProtectionRulesWithResponse(cmd.Context(), u.Repository, params, rules)
		DieOnErrorOrUnexpectedStatusCode(setResp, err, http.StatusNoContent)
	},
}

var branchProtectDeleteCmd = &cobra.Command{
	Use:               "delete <repository URI> <pattern>",
	Short:             "Delete a branch protection rule",
	Long:              "Delete a branch protection rule for a given branch name pattern",
	Example:           "lakectl branch-protect delete " + myRepoExample + " stable_*",
	Args:              cobra.ExactArgs(branchProtectDeleteCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		resp, err := client.GetBranchProtectionRulesWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		found := false
		rules := slices.DeleteFunc(*resp.JSON200, func(rule apigen.BranchProtectionRule) bool {
			if rule.Pattern == args[1] {
				found = true
				return true
			}
			return false
		})
		if !found {
			Die("Branch protection rule not found", 1)
		}
		setResp, err := client.SetBranchProtectionRulesWithResponse(cmd.Context(), u.Repository, &apigen.SetBranchProtectionRulesParams{
			IfMatch: swag.String(resp.HTTPResponse.Header.Get("ETag")),
		}, rules)
		DieOnErrorOrUnexpectedStatusCode(setResp, err, http.StatusNoContent)
	},
}

//nolint:gochecknoinits
func init() {
	// Add flags for branch protection rule configuration
	branchProtectAddCmd.Flags().Bool("block-staging-writes", true, "Block staging area writes (upload, delete objects)")
	branchProtectAddCmd.Flags().Bool("block-commits", true, "Block commits")
	branchProtectAddCmd.Flags().Bool("block-deletion", false, "Block branch deletion")

	// Allow users to disable default protections
	branchProtectAddCmd.Flags().Bool("no-block-staging-writes", false, "Disable blocking of staging area writes")
	branchProtectAddCmd.Flags().Bool("no-block-commits", false, "Disable blocking of commits")

	rootCmd.AddCommand(branchProtectCmd)
	branchProtectCmd.AddCommand(branchProtectAddCmd)
	branchProtectCmd.AddCommand(branchProtectListCmd)
	branchProtectCmd.AddCommand(branchProtectDeleteCmd)
}
