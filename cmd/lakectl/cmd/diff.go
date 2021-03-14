package cmd

import (
	"context"
	"os"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	diffCmdMinArgs  = 1
	diffCmdMaxArgs  = 2
	minDiffPageSize = 50
	maxDiffPageSize = 100000
)

var diffCmd = &cobra.Command{
	Use:   "diff <ref uri> [other ref uri]",
	Short: "diff between commits/hashes",
	Long:  "see the list of paths added/changed/removed in a branch or between two references (could be either commit hash or branch name)",
	Args: cmdutils.ValidationChain(
		cobra.RangeArgs(diffCmdMinArgs, diffCmdMaxArgs),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()

		const diffWithOtherArgsCount = 2
		if len(args) == diffWithOtherArgsCount {
			if err := uri.ValidateRefURI(args[1]); err != nil {
				DieErr(err)
			}
			leftRefURI := uri.Must(uri.Parse(args[0]))
			rightRefURI := uri.Must(uri.Parse(args[1]))
			if leftRefURI.Repository != rightRefURI.Repository {
				Die("both references must belong to the same repository", 1)
			}
			printDiffRefs(cmd.Context(), client, leftRefURI.Repository, leftRefURI.Ref, rightRefURI.Ref)
		} else {
			branchURI := uri.Must(uri.Parse(args[0]))
			printDiffBranch(cmd.Context(), client, branchURI.Repository, branchURI.Ref)
		}
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
		amount := int(pageSize)
		res, err := client.DiffBranchWithResponse(ctx, repository, branch, &api.DiffBranchParams{
			After:  &after,
			Amount: &amount,
		})
		if err != nil {
			DieErr(err)
		}
		for _, line := range *res.JSON200.Results {
			FmtDiff(line, false)
		}
		pagination := res.JSON200.Pagination
		if !pagination.HasMore {
			break
		}
		if pagination.NextOffset != nil {
			after = *pagination.NextOffset
		}
		pageSize.Next()
	}
}

func printDiffRefs(ctx context.Context, client api.ClientWithResponsesInterface, repository string, leftRef string, rightRef string) {
	var after string
	pageSize := pageSize(minDiffPageSize)
	for {
		amount := int(pageSize)
		res, err := client.DiffRefsWithResponse(ctx, repository, leftRef, rightRef, &api.DiffRefsParams{
			After:  &after,
			Amount: &amount,
		})
		if err != nil {
			DieErr(err)
		}

		for _, line := range *res.JSON200.Results {
			FmtDiff(line, true)
		}
		pagination := res.JSON200.Pagination
		if !pagination.HasMore {
			break
		}
		if pagination.NextOffset != nil {
			after = *pagination.NextOffset
		}
		pageSize.Next()
	}
}

func FmtDiff(diff api.Diff, withDirection bool) {
	var color text.Color
	var action string

	switch *diff.Type {
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
}
