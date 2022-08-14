package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	mergeCmdMinArgs = 2
	mergeCmdMaxArgs = 2

	mergeCreateTemplate = `Merged "{{.Merge.FromRef|yellow}}" into "{{.Merge.ToRef|yellow}}" to get "{{.Result.Reference|green}}".
`
)

type FromTo struct {
	FromRef, ToRef string
}

// mergeCmd represents the merge command
var mergeCmd = &cobra.Command{
	Use:   "merge <source ref> <destination ref>",
	Short: "Merge & commit changes from source branch into destination branch",
	Long:  "Merge & commit changes from source branch into destination branch",
	Args:  cobra.RangeArgs(mergeCmdMinArgs, mergeCmdMaxArgs),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		sourceRef := MustParseRefURI("source ref", args[0])
		destinationRef := MustParseRefURI("destination ref", args[1])
		strategy := MustString(cmd.Flags().GetString("strategy"))
		Fmt("Source: %s\nDestination: %s\n", sourceRef.String(), destinationRef)
		if destinationRef.Repository != sourceRef.Repository {
			Die("both references must belong to the same repository", 1)
		}

		if strategy != "dest-wins" && strategy != "source-wins" && strategy != "" {
			Die("Invalid strategy value. Expected \"dest-wins\" or \"source-wins\"", 1)
		}

		resp, err := client.MergeIntoBranchWithResponse(cmd.Context(), destinationRef.Repository, sourceRef.Ref, destinationRef.Ref, api.MergeIntoBranchJSONRequestBody{Strategy: &strategy})
		if resp != nil && resp.JSON409 != nil {
			Die("Conflict found.", 1)
		}
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)

		Write(mergeCreateTemplate, struct {
			Merge  FromTo
			Result *api.MergeResult
		}{
			Merge:  FromTo{FromRef: sourceRef.Ref, ToRef: destinationRef.Ref},
			Result: resp.JSON200,
		})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(mergeCmd)
	mergeCmd.Flags().String("strategy", "", "In case of a merge conflict, this option will force the merge process to automatically favor changes from the dest branch (\"dest-wins\") or from the source branch(\"source-wins\"). In case no selection is made, the merge process will fail in case of a conflict")
}
