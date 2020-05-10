package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	indexerrors "github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/uri"
)

// mergeCmd represents the merge command
var mergeCmd = &cobra.Command{
	Use:   "merge",
	Short: "merge  [source ref] [destination ref] ",
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
		if err == nil {
			var added, changed, removed int
			for _, r := range result {
				switch r.Type {
				case models.DiffTypeADDED:
					added++
				case models.DiffTypeCHANGED:
					changed++
				case models.DiffTypeREMOVED:
					removed++
				}
			}
			_, _ = os.Stdout.WriteString(fmt.Sprintf("new: %d modified: %d removed: %d \n", added, changed, removed))
		} else if err == indexerrors.ErrMergeConflict {
			_, _ = os.Stdout.WriteString(" Conflicts:\n")
			for _, line := range result {
				if line.Direction == models.DiffDirectionCONFLICT {
					FmtMerge(line)
				}
			}
		} else {
			DieErr(err)
		}
	},
}

func FmtMerge(diff *models.MergeResult) {
	var color text.Color
	var action string

	switch diff.Type {
	case models.DiffTypeADDED:
		color = text.FgGreen
		action = "+ added"
	case models.DiffTypeREMOVED:
		color = text.FgRed
		action = "- removed"
	default:
		color = text.FgYellow
		action = "~ modified"
	}

	_, _ = os.Stdout.WriteString(
		color.Sprintf("    %s %s \n", action, diff.Path),
	)
}

func init() {
	rootCmd.AddCommand(mergeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// mergeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mergeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
