package cmd

import (
	"fmt"
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var branchMoveToCommitCmd = &cobra.Command{
	Use:               "move-to-commit <branch-uri> <commit-ref>",
	Short:             "Move branch pointer to a specific commit",
	Long:              `Move a branch pointer to reference a specific commit. This operation is destructive and cannot be undone.`,
	Example:           "lakectl branch move-to-commit lakefs://example-repo/my-branch 1234567890abcdef",
	Args:              cobra.ExactArgs(2),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		branchURI := args[0]
		commitRef := args[1]

		// Parse and validate branch URI
		u := MustParseBranchURI("branch URI", branchURI)

		// Get API client
		client := getClient()
		ctx := cmd.Context()

		// Check if branch exists and get current state
		branchResp, err := client.GetBranchWithResponse(ctx, u.Repository, u.Ref)
		DieOnErrorOrUnexpectedStatusCode(branchResp, err, http.StatusOK)

		// Check if target commit exists
		commitResp, err := client.GetCommitWithResponse(ctx, u.Repository, commitRef)
		DieOnErrorOrUnexpectedStatusCode(commitResp, err, http.StatusOK)

		// Check for uncommitted changes
		diffResp, err := client.DiffBranchWithResponse(ctx, u.Repository, u.Ref, &apigen.DiffBranchParams{})
		DieOnErrorOrUnexpectedStatusCode(diffResp, err, http.StatusOK)

		hasUncommittedChanges := len(diffResp.JSON200.Results) > 0
		forceFlag, err := cmd.Flags().GetBool("force")
		if err != nil {
			DieErr(err)
		}

		// Check if force is required
		if hasUncommittedChanges && !forceFlag {
			DieFmt("Branch '%s' has uncommitted changes. Use --force to proceed and discard them.", u.Ref)
		}

		// Build confirmation message
		var confirmationMsg string
		if hasUncommittedChanges {
			fmt.Printf("WARNING: Branch '%s' has uncommitted changes that will be permanently lost.\n", u.Ref)
			confirmationMsg = fmt.Sprintf("Are you sure you want to move branch '%s' to commit '%s'", u.Ref, commitRef)
		} else {
			confirmationMsg = fmt.Sprintf("Are you sure you want to move branch '%s' to commit '%s'", u.Ref, commitRef)
		}

		// Get confirmation from user
		confirmation, err := Confirm(cmd.Flags(), confirmationMsg)
		if err != nil || !confirmation {
			Die("Operation aborted", 1)
			return
		}

		// Prepare API request
		params := &apigen.HardResetBranchParams{
			Ref: commitRef,
		}
		if hasUncommittedChanges {
			params.Force = swag.Bool(true)
		}

		// Execute hard reset
		resetResp, err := client.HardResetBranchWithResponse(ctx, u.Repository, u.Ref, params)
		DieOnErrorOrUnexpectedStatusCode(resetResp, err, http.StatusNoContent)

		// Success message
		fmt.Printf("Successfully moved branch '%s' to commit '%s'\n", u.Ref, commitRef)
	},
}

//nolint:gochecknoinits
func init() {
	// Add flags
	branchMoveToCommitCmd.Flags().BoolP("force", "f", false, "Force the operation even if the branch has uncommitted changes")
	AssignAutoConfirmFlag(branchMoveToCommitCmd.Flags())

	// Add to branch command
	branchCmd.AddCommand(branchMoveToCommitCmd)
}
