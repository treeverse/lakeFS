package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/diff"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	diffCmdMinArgs = 1
	diffCmdMaxArgs = 2

	minDiffPageSize = 50
	maxDiffPageSize = 1000

	twoWayFlagName = "two-way"
	prefixFlagName = "prefix"
)

var diffCmd = &cobra.Command{
	Use:   `diff <ref URI> [ref URI]`,
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
	Show changes between the tip of the main and the dev branch, including uncommitted changes on dev.
	
	lakectl diff --%s some/path lakefs://example-repo/main lakefs://example-repo/dev
	Show changes of objects prefixed with 'some/path' between the tips of the main and dev branches.`, twoWayFlagName, twoWayFlagName, prefixFlagName),

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
			branchURI := MustParseBranchURI("branch URI", args[0])
			fmt.Println("Ref:", branchURI)
			printDiffBranch(cmd.Context(), client, branchURI.Repository, branchURI.Ref)
			return
		}

		twoWay := Must(cmd.Flags().GetBool(twoWayFlagName))
		prefix := Must(cmd.Flags().GetString(prefixFlagName))
		leftRefURI := MustParseRefURI("left ref", args[0])
		rightRefURI := MustParseRefURI("right ref", args[1])
		fmt.Printf("Left ref: %s\nRight ref: %s\n", leftRefURI, rightRefURI)
		if leftRefURI.Repository != rightRefURI.Repository {
			Die("both references must belong to the same repository", 1)
		}
		printDiffRefs(cmd.Context(), client, leftRefURI, rightRefURI, twoWay, prefix)
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

func printDiffBranch(ctx context.Context, client apigen.ClientWithResponsesInterface, repository string, branch string) {
	var after string
	pageSize := pageSize(minDiffPageSize)
	for {
		resp, err := client.DiffBranchWithResponse(ctx, repository, branch, &apigen.DiffBranchParams{
			After:  apiutil.Ptr(apigen.PaginationAfter(after)),
			Amount: apiutil.Ptr(apigen.PaginationAmount(pageSize)),
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

func printDiffRefs(ctx context.Context, client apigen.ClientWithResponsesInterface, left, right *uri.URI, twoDot bool, prefix string) {
	diffs := make(chan apigen.Diff, maxDiffPageSize)
	var wg errgroup.Group
	wg.Go(func() error {
		return diff.StreamRepositoryDiffs(ctx, client, left, right, prefix, diffs, twoDot)
	})
	for d := range diffs {
		FmtDiff(d, true)
	}
	if err := wg.Wait(); err != nil {
		DieErr(err)
	}
}

func FmtDiff(d apigen.Diff, withDirection bool) {
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
	diffCmd.Flags().String(prefixFlagName, "", "Show only changes in the given prefix.")
	rootCmd.AddCommand(diffCmd)
}
