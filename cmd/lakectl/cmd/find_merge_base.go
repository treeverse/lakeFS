package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	findMergeBaseCmdExactArgs = 2

	findMergeBaseTemplate = `Found base "{{.Merge.BaseRef|yellow}}" when merging "{{.Merge.FromRef|yellow}}" into "{{.Merge.ToRef|yellow}}".
`
)

type FromToBase struct {
	BaseRef string
	FromRef string
	ToRef   string
}

var findMergeBaseCmd = &cobra.Command{
	Hidden: true,
	Use:    "find-merge-base <source ref> <destination ref>",
	Short:  "Find the commits for the merge operation",
	Args:   cobra.ExactArgs(findMergeBaseCmdExactArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) >= mergeCmdMaxArgs {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return validRepositoryToComplete(cmd.Context(), toComplete)
	},
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		sourceRef := utils.MustParseRefURI("source ref", args[0])
		destinationRef := utils.MustParseRefURI("destination ref", args[1])
		utils.Fmt("Source: %s\nDestination: %s\n", sourceRef, destinationRef)
		if destinationRef.Repository != sourceRef.Repository {
			utils.Die("both references must belong to the same repository", 1)
		}

		resp, err := client.FindMergeBaseWithResponse(cmd.Context(), destinationRef.Repository, sourceRef.Ref, destinationRef.Ref)
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			utils.Die("Bad response from server", 1)
		}

		utils.Write(findMergeBaseTemplate, struct {
			Merge  FromToBase
			Result *api.FindMergeBaseResult
		}{
			Merge: FromToBase{
				FromRef: resp.JSON200.SourceCommitId,
				ToRef:   resp.JSON200.DestinationCommitId,
				BaseRef: resp.JSON200.BaseCommitId,
			},
			Result: resp.JSON200,
		})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(findMergeBaseCmd)
}
