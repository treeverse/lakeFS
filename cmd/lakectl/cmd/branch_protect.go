package cmd

import (
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"golang.org/x/exp/slices"
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
		patterns := make([][]interface{}, len(*resp.JSON200))
		for i, rule := range *resp.JSON200 {
			patterns[i] = []interface{}{rule.Pattern}
		}
		PrintTable(patterns, []interface{}{"Branch Name Pattern"}, &apigen.Pagination{
			HasMore: false,
			Results: len(patterns),
		}, len(patterns))
	},
}

var branchProtectAddCmd = &cobra.Command{
	Use:               "add <repository URI> <pattern>",
	Short:             "Add a branch protection rule",
	Long:              "Add a branch protection rule for a given branch name pattern",
	Example:           "lakectl branch-protect add " + myRepoExample + " 'stable_*'",
	Args:              cobra.ExactArgs(branchProtectAddCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		resp, err := client.GetBranchProtectionRulesWithResponse(cmd.Context(), u.Repository)

		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		rules := *resp.JSON200
		rules = append(rules, apigen.BranchProtectionRule{
			Pattern: args[1],
		})
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
	rootCmd.AddCommand(branchProtectCmd)
	branchProtectCmd.AddCommand(branchProtectAddCmd)
	branchProtectCmd.AddCommand(branchProtectListCmd)
	branchProtectCmd.AddCommand(branchProtectDeleteCmd)
}
