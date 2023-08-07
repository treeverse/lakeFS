package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/diff"
)

const (
	diffCmdMinArgs = 1
	diffCmdMaxArgs = 2

	minDiffPageSize = 50
	maxDiffPageSize = 1000

	twoWayFlagName = "two-way"
)

var diffCmd = &cobra.Command{
	Use:   `diff <ref uri> [ref uri]`,
	Short: "Show changes between two commits, or the currently uncommitted changes",
	Example: fmt.Sprintf(`
	lakectl diff lakefs://example-repo/example-branch
	Show uncommitted changes in example-branch.

	lakectl diff lakefs://example-repo/main lakefs://example-repo/dev
	This shows the differences between master and dev starting at the last common commit.
	This is similar to the three-dot (...) syntax in git.
	Uncommitted changes are not shown.

	lakectl diff --%s lakefs://example-repo/main lakefs://example-repo/dev
	Show changes between the tips of the main and dev branches.
	This is similar to the two-dot (..) syntax in git.
	Uncommitted changes are not shown.

	lakectl diff --%s lakefs://example-repo/main lakefs://example-repo/dev$
	Show changes between the tip of the main and the dev branch, including uncommitted changes on dev.`, twoWayFlagName, twoWayFlagName),

	Args: cobra.RangeArgs(diffCmdMinArgs, diffCmdMaxArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) >= diffCmdMaxArgs {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return validRepositoryToComplete(cmd.Context(), toComplete)
	},
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		if len(args) == diffCmdMinArgs {
			// got one arg ref: uncommitted changes diff
			branchURI := MustParseRefURI("ref", args[0])
			fmt.Println("Ref:", branchURI)
			printDiffBranch(cmd.Context(), client, branchURI.Repository, branchURI.Ref)
			return
		}

		twoWay := Must(cmd.Flags().GetBool(twoWayFlagName))
		leftRefURI := MustParseRefURI("left ref", args[0])
		rightRefURI := MustParseRefURI("right ref", args[1])
		fmt.Printf("Left ref: %s\nRight ref: %s\n", leftRefURI, rightRefURI)
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
	diffs := make(chan api.Diff, maxDiffPageSize)
	err := diff.GetDiffRefs(ctx, client, repository, leftRef, rightRef, "", diffs, twoDot)
	if err != nil {
		DieErr(err)
	}
	for d := range diffs {
		FmtDiff(d, true)
	}
}

func FmtDiff(d api.Diff, withDirection bool) {
	action, color := diff.Fmt(d.Type)

	if !withDirection {
		_, _ = os.Stdout.WriteString(
			color.Sprintf("%s %s\n", action, d.Path),
		)
		return
	}
	_, _ = os.Stdout.WriteString(
		color.Sprintf("%s %s\n", action, d.Path),
	)
}

//nolint:gochecknoinits
func init() {
	diffCmd.Flags().Bool(twoWayFlagName, false, "Use two-way diff: show difference between the given refs, regardless of a common ancestor.")

	rootCmd.AddCommand(diffCmd)
}
