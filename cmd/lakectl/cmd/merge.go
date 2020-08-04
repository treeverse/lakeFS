package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/treeverse/lakefs/catalog"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/uri"
)

// mergeCmd represents the merge command
var mergeCmd = &cobra.Command{
	Use:   "merge <source ref> <destination ref>",
	Short: "merge",
	Long:  "merge & commit changes from source branch into destination branch",
	Args: ValidationChain(
		HasRangeArgs(2, 2),
		IsRefURI(0),
		IsRefURI(1),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		if err := IsRefURI(1)(args); err != nil {
			DieErr(err)
		}
		rightRefURI := uri.Must(uri.Parse(args[0]))
		leftRefURI := uri.Must(uri.Parse(args[1]))

		if leftRefURI.Repository != rightRefURI.Repository {
			Die("both references must belong to the same repository", 1)
		}

		result, err := client.Merge(context.Background(), leftRefURI.Repository, leftRefURI.Ref, rightRefURI.Ref)
		if errors.Is(err, catalog.ErrConflictFound) {
			_, _ = os.Stdout.WriteString("Conflicts:\n")
			for _, line := range result {
				if line.Type == models.DiffTypeConflict {
					FmtMerge(line)
				}
			}
			return
		}
		if err != nil {
			DieErr(err)
		}
		var added, changed, removed int
		for _, r := range result {
			switch r.Type {
			case models.DiffTypeAdded:
				added++
			case models.DiffTypeChanged:
				changed++
			case models.DiffTypeRemoved:
				removed++
			}
		}
		_, _ = os.Stdout.WriteString(fmt.Sprintf("new: %d modified: %d removed: %d\n", added, changed, removed))
	},
}

func FmtMerge(diff *models.MergeResult) {
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
		color = text.FgHiRed
		action = ". other"
	}

	_, _ = os.Stdout.WriteString(color.Sprintf("    %s %s\n", action, diff.Path))
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(mergeCmd)
}
