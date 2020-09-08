package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/catalog"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/uri"
)

const (
	mergeCmdMinArgs = 2
	mergeCmdMaxArgs = 2
)

// mergeCmd represents the merge command
var mergeCmd = &cobra.Command{
	Use:   "merge <source ref> <destination ref>",
	Short: "merge",
	Long:  "merge & commit changes from source branch into destination branch",
	Args: ValidationChain(
		HasRangeArgs(mergeCmdMinArgs, mergeCmdMaxArgs),
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
			var count int64
			for _, item := range result.Summary {
				if item.Type == models.DiffTypeConflict {
					count = item.Count
					break
				}
			}
			_, _ = fmt.Printf("Conflicts: %d\n", count)
			return
		}
		if err != nil {
			DieErr(err)
		}
		var added, changed, removed int64
		for _, item := range result.Summary {
			switch item.Type {
			case models.DiffTypeAdded:
				added = item.Count
			case models.DiffTypeChanged:
				changed = item.Count
			case models.DiffTypeRemoved:
				removed = item.Count
			}
		}
		_, _ = fmt.Printf("new: %d modified: %d removed: %d\n", added, changed, removed)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(mergeCmd)
}
