package cmd

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	requiredArgsCount = 2
)

var (
	errBranchURIEmpty          = errors.New("branch URI cannot be empty")
	errCommitReferenceEmpty    = errors.New("commit reference cannot be empty")
	errBranchURIInvalidScheme  = errors.New("branch URI must start with 'lakefs://'")
	errBranchURIInvalidChars   = errors.New("branch URI contains invalid characters")
	errCommitRefInvalidChars   = errors.New("commit reference contains invalid characters")
	errCommitRefInvalidFormat  = errors.New("commit reference contains invalid characters (allowed: alphanumeric, ., _, /, ~, -)")
	errBranchURITooLong        = errors.New("branch URI too long")
	errCommitRefTooLong        = errors.New("commit reference too long")
)

var branchMoveToCommitCmd = &cobra.Command{
	Use:               "move-to-commit <branch-uri> <commit-ref>",
	Short:             "Move branch pointer to a specific commit",
	Long:              `Move a branch pointer to reference a specific commit. This operation is destructive and cannot be undone.`,
	Example:           "lakectl branch move-to-commit lakefs://example-repo/my-branch 1234567890abcdef",
	Args:              cobra.ExactArgs(requiredArgsCount),
	ValidArgsFunction: ValidArgsRepository,
	Hidden:            true,
	Run: func(cmd *cobra.Command, args []string) {
		branchURI := strings.TrimSpace(args[0])
		commitRef := strings.TrimSpace(args[1])

		// Input validation and sanitization
		if err := validateInputs(branchURI, commitRef); err != nil {
			DieFmt("Invalid input: %s", err)
		}

		// Parse and validate branch URI
		u := MustParseBranchURI("branch URI", branchURI)

		// Get API client
		client := getClient()
		ctx := cmd.Context()

		// Check if branch exists and get current state
		branchResp, err := client.GetBranchWithResponse(ctx, u.Repository, u.Ref)
		if err != nil {
			DieFmt("Failed to access branch '%s': %s", u.Ref, err)
		}
		if branchResp.StatusCode() == http.StatusNotFound {
			DieFmt("Branch '%s' not found in repository '%s'", u.Ref, u.Repository)
		}
		if branchResp.StatusCode() == http.StatusForbidden {
			DieFmt("Access denied: insufficient permissions to access branch '%s'", u.Ref)
		}
		DieOnErrorOrUnexpectedStatusCode(branchResp, err, http.StatusOK)

		// Check if target commit exists
		commitResp, err := client.GetCommitWithResponse(ctx, u.Repository, commitRef)
		if err != nil {
			DieFmt("Failed to access commit '%s': %s", commitRef, err)
		}
		if commitResp.StatusCode() == http.StatusNotFound {
			DieFmt("Commit '%s' not found in repository '%s'", commitRef, u.Repository)
		}
		if commitResp.StatusCode() == http.StatusForbidden {
			DieFmt("Access denied: insufficient permissions to access commit '%s'", commitRef)
		}
		DieOnErrorOrUnexpectedStatusCode(commitResp, err, http.StatusOK)

		// Check for uncommitted changes
		diffResp, err := client.DiffBranchWithResponse(ctx, u.Repository, u.Ref, &apigen.DiffBranchParams{})
		if err != nil {
			DieFmt("Failed to check for uncommitted changes on branch '%s': %s", u.Ref, err)
		}
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
		if err != nil {
			DieFmt("Failed to move branch '%s' to commit '%s': %s", u.Ref, commitRef, err)
		}
		if resetResp.StatusCode() == http.StatusForbidden {
			DieFmt("Access denied: insufficient permissions to move branch '%s'. Required permission: HardResetBranch", u.Ref)
		}
		if resetResp.StatusCode() == http.StatusConflict {
			DieFmt("Branch '%s' has uncommitted changes. Use --force to proceed and discard them.", u.Ref)
		}
		if resetResp.StatusCode() == http.StatusPreconditionFailed {
			DieFmt("Branch '%s' is protected and cannot be moved", u.Ref)
		}
		DieOnErrorOrUnexpectedStatusCode(resetResp, err, http.StatusNoContent)

		// Success message
		fmt.Printf("Successfully moved branch '%s' to commit '%s'\n", u.Ref, commitRef)
	},
}

// validateInputs performs comprehensive input validation and sanitization
func validateInputs(branchURI, commitRef string) error {
	// Check for empty inputs
	if branchURI == "" {
		return errBranchURIEmpty
	}
	if commitRef == "" {
		return errCommitReferenceEmpty
	}

	// Validate branch URI format (basic checks before parsing)
	if !strings.HasPrefix(branchURI, "lakefs://") {
		return errBranchURIInvalidScheme
	}

	// Check for suspicious characters that might indicate injection attempts
	suspiciousChars := []string{"\n", "\r", "\t", "\x00", "\x1f"}
	for _, char := range suspiciousChars {
		if strings.Contains(branchURI, char) {
			return errBranchURIInvalidChars
		}
		if strings.Contains(commitRef, char) {
			return errCommitRefInvalidChars
		}
	}

	// Validate commit reference format (Git commit hash or branch name)
	// Allow: alphanumeric, hyphens, underscores, slashes, dots, tildes (~)
	commitRefPattern := regexp.MustCompile(`^[a-zA-Z0-9._/~-]+$`)
	if !commitRefPattern.MatchString(commitRef) {
		return errCommitRefInvalidFormat
	}

	// Check reasonable length limits to prevent DoS
	const maxBranchURILength = 1000
	const maxCommitRefLength = 500
	if len(branchURI) > maxBranchURILength {
		return fmt.Errorf("%w (max %d characters)", errBranchURITooLong, maxBranchURILength)
	}
	if len(commitRef) > maxCommitRefLength {
		return fmt.Errorf("%w (max %d characters)", errCommitRefTooLong, maxCommitRefLength)
	}

	return nil
}

//nolint:gochecknoinits
func init() {
	// Add flags
	branchMoveToCommitCmd.Flags().BoolP("force", "f", false, "Force the operation even if the branch has uncommitted changes")
	AssignAutoConfirmFlag(branchMoveToCommitCmd.Flags())

	// Add to branch command
	branchCmd.AddCommand(branchMoveToCommitCmd)
}
