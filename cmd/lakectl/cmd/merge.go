package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
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
	Args: cmdutils.ValidationChain(
		cobra.RangeArgs(mergeCmdMinArgs, mergeCmdMaxArgs),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
		cmdutils.FuncValidator(1, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		rightRefURI := uri.Must(uri.Parse(args[0]))
		leftRefURI := uri.Must(uri.Parse(args[1]))

		if leftRefURI.Repository != rightRefURI.Repository {
			Die("both references must belong to the same repository", 1)
		}

		result, err := client.Merge(context.Background(), leftRefURI.Repository, leftRefURI.Ref, rightRefURI.Ref)
		if errors.Is(err, catalog.ErrConflictFound) {
			_, _ = fmt.Printf("Conflicts: %d\n", result.Summary.Conflict)
			return
		}
		if err != nil {
			DieErr(err)
		}
		_, _ = fmt.Printf("new: %d modified: %d removed: %d\n", result.Summary.Added, result.Summary.Changed, result.Summary.Removed)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(mergeCmd)
}
