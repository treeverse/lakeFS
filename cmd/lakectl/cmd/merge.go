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
	Use:   "merge <destination ref> <source ref>",
	Short: "merge",
	Long:  "merge & commit changes from source branch into destination branch",
	Args: cmdutils.ValidationChain(
		cobra.RangeArgs(mergeCmdMinArgs, mergeCmdMaxArgs),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
		cmdutils.FuncValidator(1, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		destinationRef := uri.Must(uri.Parse(args[0]))
		sourceRef := uri.Must(uri.Parse(args[1]))

		if destinationRef.Repository != sourceRef.Repository {
			Die("both references must belong to the same repository", 1)
		}

		result, err := client.Merge(context.Background(), destinationRef.Repository, destinationRef.Ref, sourceRef.Ref)
		if errors.Is(err, catalog.ErrConflictFound) {
			DieFmt("%d conflict(s) found\n", result.Summary.Conflict)
		}
		if err != nil {
			DieErr(err)
		}
		fmt.Printf("Merged %s\n", result.Reference)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(mergeCmd)
}
