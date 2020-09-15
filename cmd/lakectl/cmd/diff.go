package cmd

import (
	"context"
	"os"

	"github.com/go-openapi/swag"
	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/uri"
)

const (
	diffCmdMinArgs = 1
	diffCmdMaxArgs = 2
	diffPageSize   = 100
)

var diffCmd = &cobra.Command{
	Use:   "diff <ref uri> [other ref uri]",
	Short: "diff between commits/hashes",
	Long:  "see the list of paths added/changed/removed in a branch or between two references (could be either commit hash or branch name)",
	Args: ValidationChain(
		HasRangeArgs(diffCmdMinArgs, diffCmdMaxArgs),
		IsRefURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()

		const diffWithOtherArgsCount = 2
		if len(args) == diffWithOtherArgsCount {
			if err := IsRefURI(1)(args); err != nil {
				DieErr(err)
			}
			leftRefURI := uri.Must(uri.Parse(args[0]))
			rightRefURI := uri.Must(uri.Parse(args[1]))
			if leftRefURI.Repository != rightRefURI.Repository {
				Die("both references must belong to the same repository", 1)
			}
			printDiffRefs(client, leftRefURI.Repository, leftRefURI.Ref, rightRefURI.Ref)
		} else {
			branchURI := uri.Must(uri.Parse(args[0]))
			printDiffBranch(client, branchURI.Repository, branchURI.Ref)
		}
	},
}

func printDiffBranch(client api.Client, repository string, branch string) {
	var after string
	for {
		diff, pagination, err := client.DiffBranch(context.Background(), repository, branch, after, diffPageSize)
		if err != nil {
			DieErr(err)
		}
		for _, line := range diff {
			FmtDiff(line, false)
		}
		if !swag.BoolValue(pagination.HasMore) {
			break
		}
		after = pagination.NextOffset
	}
}

func printDiffRefs(client api.Client, repository string, leftRef string, rightRef string) {
	var after string
	for {
		diff, pagination, err := client.DiffRefs(context.Background(), repository, leftRef, rightRef,
			after, diffPageSize)
		if err != nil {
			DieErr(err)
		}
		for _, line := range diff {
			FmtDiff(line, true)
		}
		if !swag.BoolValue(pagination.HasMore) {
			break
		}
		after = pagination.NextOffset
	}
}

func FmtDiff(diff *models.Diff, withDirection bool) {
	var color text.Color
	var action string

	switch diff.Type {
	case models.DiffTypeAdded:
		color = text.FgGreen
		action = "+ added"
	case models.DiffTypeRemoved:
		color = text.FgRed
		action = "- removed"
	case models.DiffTypeChanged:
		color = text.FgYellow
		action = "~ modified"
	case models.DiffTypeConflict:
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
