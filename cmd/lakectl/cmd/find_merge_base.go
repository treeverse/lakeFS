package cmd

import (
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	findMergeBaseCmdMinArgs = 2
	findMergeBaseCmdMaxArgs = 2

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
	Args:   cobra.RangeArgs(findMergeBaseCmdMinArgs, findMergeBaseCmdMaxArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) >= mergeCmdMaxArgs {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return validRepositoryToComplete(cmd.Context(), toComplete)
	},
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		sourceRef := MustParseRefURI("source ref", args[0])
		destinationRef := MustParseRefURI("destination ref", args[1])
		Fmt("Source: %s\nDestination: %s\n", sourceRef.String(), destinationRef)
		if destinationRef.Repository != sourceRef.Repository {
			Die("both references must belong to the same repository", 1)
		}

		resp, err := client.FindMergeBaseWithResponse(cmd.Context(), destinationRef.Repository, sourceRef.Ref, destinationRef.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		Write(findMergeBaseTemplate, struct {
			Merge  FromToBase
			Result *api.FindMergeBaseResult
		}{
			Merge: FromToBase{
				FromRef: swag.StringValue(resp.JSON200.SourceCommitID),
				ToRef:   swag.StringValue(resp.JSON200.DestinationCommitID),
				BaseRef: swag.StringValue(resp.JSON200.BaseCommitID),
			},
			Result: resp.JSON200,
		})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(findMergeBaseCmd)
}
