package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

const (
	mergeCmdMinArgs = 2
	mergeCmdMaxArgs = 2
)

var mergeCreateTemplate = `Merged "{{.Merge.FromRef|yellow}}" into "{{.Merge.ToRef|yellow}}" to get "{{.Result.Reference|green}}".

Added: {{.Result.Summary.Added}}
Changed: {{.Result.Summary.Changed}}
Removed: {{.Result.Summary.Removed}}

`

type FromTo struct {
	FromRef, ToRef string
}

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
			_, _ = fmt.Printf("Conflicts: %d\n", result.Summary.Conflict)
			return
		}
		if err != nil {
			DieErr(err)
		}

		Write(mergeCreateTemplate, struct {
			Merge  FromTo
			Result *models.MergeResult
		}{
			FromTo{FromRef: sourceRef.Ref, ToRef: destinationRef.Ref},
			result,
		})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(mergeCmd)
}
