package cmd

import (
	"context"
	"os"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/uri"
)

var diffCmd = &cobra.Command{
	Use:   "diff <ref uri> [other ref uri]",
	Short: "diff between commits/hashes",
	Long:  "see the list of paths added/changed/removed in a branch or between two references (could be either commit hash or branch name)",
	Args: ValidationChain(
		HasRangeArgs(1, 2),
		IsRefURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()

		var diff []*models.Diff
		var err error
		if len(args) == 2 {
			if err := IsRefURI(1)(args); err != nil {
				DieErr(err)
			}
			leftRefURI := uri.Must(uri.Parse(args[0]))
			rightRefURI := uri.Must(uri.Parse(args[1]))

			if leftRefURI.Repository != rightRefURI.Repository {
				Die("both references must belong to the same repository", 1)
			}

			diff, err = client.DiffRefs(context.Background(), leftRefURI.Repository, leftRefURI.Ref, rightRefURI.Ref)
			if err != nil {
				DieErr(err)
			}
			for _, line := range diff {
				FmtDiff(line, true)
			}
		} else {
			branchURI := uri.Must(uri.Parse(args[0]))
			diff, err = client.DiffBranch(context.Background(), branchURI.Repository, branchURI.Ref)
			if err != nil {
				DieErr(err)
			}
			for _, line := range diff {
				FmtDiff(line, false)
			}
		}
	},
}

func FmtDiff(diff *models.Diff, withDirection bool) {
	var color text.Color
	var action string

	switch diff.Type {
	case models.DiffTypeADDED:
		color = text.FgGreen
		action = "+ added"
	case models.DiffTypeREMOVED:
		color = text.FgRed
		action = "- removed"
	case models.DiffTypeCHANGED:
		color = text.FgYellow
		action = "~ modified"
	case models.DiffTypeCONFLICT:
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

func init() {
	rootCmd.AddCommand(diffCmd)
}
