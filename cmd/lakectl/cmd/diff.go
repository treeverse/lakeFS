package cmd

import (
	"context"
	"os"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	diffCmdMinArgs = 1
	diffCmdMaxArgs = 2

	minDiffPageSize = 50
	maxDiffPageSize = 100000

	twoWayFlagName = "two-way"
	diffTypeTwoDot = "two_dot"
)

var diffCmd = &cobra.Command{
	Use:   `diff <ref uri> [ref uri]`,
	Short: "Show changes between two commits, or the currently uncommitted changes",
	Example: `
	lakectl diff lakefs://example-repo/example-branch
	Show uncommitted changes in example-branch.

	lakectl diff lakefs://example-repo/main lakefs://example-repo/dev
	This shows the differences between master and dev starting at the last common commit.
	This is similar to the three-dot (...) syntax in git.
	Uncommitted changes are not shown.

	lakectl diff lakefs://example-repo/main..lakefs://example-repo/dev
	Show changes between the tips of the main and dev branches.
	This is similar to the two-dot (..) syntax in git.
	Uncommitted changes are not shown.

	lakectl diff lakefs://example-repo/main..lakefs://example-repo/dev$
	Show changes between the tip of the main and the dev branch, including uncommitted changes on dev.`,

	Args: cobra.RangeArgs(diffCmdMinArgs, diffCmdMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		if len(args) == diffCmdMinArgs {
			// got one arg ref: uncommitted changes diff
			branchURI := MustParseRefURI("ref", args[0])
			Fmt("Ref: %s\n", branchURI.String())
			printDiffBranch(cmd.Context(), client, branchURI.Repository, branchURI.Ref)
			return
		}

		twoWay, _ := cmd.Flags().GetBool(twoWayFlagName)
		leftRefURI := MustParseRefURI("left ref", args[0])
		rightRefURI := MustParseRefURI("right ref", args[1])
		Fmt("Left ref: %s\nRight ref: %s\n", leftRefURI.String(), rightRefURI.String())
		if leftRefURI.Repository != rightRefURI.Repository {
			Die("both references must belong to the same repository", 1)
		}
		printDiffRefs(cmd.Context(), client, leftRefURI.Repository, leftRefURI.Ref, rightRefURI.Ref, twoWay)
	},
}

type pageSize int

func (p *pageSize) Value() int { return int(*p) }

func (p *pageSize) Next() int {
	*p *= 2
	if *p > maxDiffPageSize {
		*p = maxDiffPageSize
	}
	return p.Value()
}

func printDiffBranch(ctx context.Context, client api.ClientWithResponsesInterface, repository string, branch string) {
	var after string
	pageSize := pageSize(minDiffPageSize)
	for {
		resp, err := client.DiffBranchWithResponse(ctx, repository, branch, &api.DiffBranchParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(int(pageSize)),
		})
		DieOnResponseError(resp, err)

		for _, line := range resp.JSON200.Results {
			FmtDiff(line, false)
		}
		pagination := resp.JSON200.Pagination
		if !pagination.HasMore {
			break
		}
		after = pagination.NextOffset
		pageSize.Next()
	}
}

func printDiffRefs(ctx context.Context, client api.ClientWithResponsesInterface, repository string, leftRef string, rightRef string, twoDot bool) {
	var diffType *string
	if twoDot {
		diffType = api.StringPtr(diffTypeTwoDot)
	}
	var after string
	pageSize := pageSize(minDiffPageSize)
	for {
		amount := int(pageSize)
		resp, err := client.DiffRefsWithResponse(ctx, repository, leftRef, rightRef, &api.DiffRefsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
			Type:   diffType,
		})
		DieOnResponseError(resp, err)

		for _, line := range resp.JSON200.Results {
			FmtDiff(line, true)
		}
		pagination := resp.JSON200.Pagination
		if !pagination.HasMore {
			break
		}
		after = pagination.NextOffset
		pageSize.Next()
	}
}

func FmtDiff(diff api.Diff, withDirection bool) {
	var color text.Color
	var action string

	switch diff.Type {
	case "added":
		color = text.FgGreen
		action = "+ added"
	case "removed":
		color = text.FgRed
		action = "- removed"
	case "changed":
		color = text.FgYellow
		action = "~ modified"
	case "conflict":
		color = text.FgHiYellow
		action = "* conflict"
	default:
	}

	if !withDirection {
		_, _ = os.Stdout.WriteString(
			color.Sprintf("%s %s\n", action, diff.Path),
		)
		return
	}

	_, _ = os.Stdout.WriteString(
		color.Sprintf("%s %s\n", action, diff.Path),
	)
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(diffCmd)
	diffCmd.Flags().Bool(twoWayFlagName, false, "Use two way diff")
}
