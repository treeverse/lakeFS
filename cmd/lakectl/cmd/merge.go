package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	mergeCmdMinArgs = 2
	mergeCmdMaxArgs = 6

	mergeCreateTemplate = `Merged "{{.Merge.FromRef|yellow}}" into "{{.Merge.ToRef|yellow}}" to get "{{.Result.Reference|green}}".
`
)

type FromTo struct {
	FromRef string
	ToRef   string
}

// mergeCmd represents the merge command
var mergeCmd = &cobra.Command{
	Use:   "merge <source ref> <destination ref>",
	Short: "Merge & commit changes from source branch into destination branch",
	Long:  "Merge & commit changes from source branch into destination branch",
	Args:  cobra.RangeArgs(mergeCmdMinArgs, mergeCmdMaxArgs),
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) >= mergeCmdMaxArgs {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return validRepositoryToComplete(cmd.Context(), toComplete)
	},
	Run: func(cmd *cobra.Command, args []string) {
		message, kvPairs := getCommitFlags(cmd)
		client := getClient()

		sourceRef := MustParseBranchURI("source ref", args[0])
		destinationRef := MustParseBranchURI("destination ref", args[1])
		strategy := Must(cmd.Flags().GetString("strategy"))
		force := Must(cmd.Flags().GetBool("force"))
		allowEmpty := Must(cmd.Flags().GetBool("allow-empty"))

		fmt.Println("Source:", sourceRef)
		fmt.Println("Destination:", destinationRef)

		if destinationRef.Repository != sourceRef.Repository {
			Die("both references must belong to the same repository", 1)
		}
		if strategy != "dest-wins" && strategy != "source-wins" && strategy != "" {
			Die("Invalid strategy value. Expected \"dest-wins\" or \"source-wins\"", 1)
		}

		body := apigen.MergeIntoBranchJSONRequestBody{
			Message:    &message,
			Metadata:   &apigen.Merge_Metadata{AdditionalProperties: kvPairs},
			Strategy:   &strategy,
			Force:      &force,
			AllowEmpty: &allowEmpty,
		}

		resp, err := client.MergeIntoBranchWithResponse(cmd.Context(), destinationRef.Repository, sourceRef.Ref, destinationRef.Ref, body)
		if resp != nil && resp.JSON409 != nil {
			Die("Conflict found.", 1)
		}
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		Write(mergeCreateTemplate, struct {
			Merge  FromTo
			Result *apigen.MergeResult
		}{
			Merge: FromTo{
				FromRef: sourceRef.Ref,
				ToRef:   destinationRef.Ref,
			},
			Result: resp.JSON200,
		})
	},
}

//nolint:gochecknoinits
func init() {
	flags := mergeCmd.Flags()
	flags.String("strategy", "", "In case of a merge conflict, this option will force the merge process to automatically favor changes from the dest branch (\"dest-wins\") or from the source branch(\"source-wins\"). In case no selection is made, the merge process will fail in case of a conflict")
	flags.Bool("force", false, "Allow merge into a read-only branch or into a branch with the same content")
	flags.Bool("allow-empty", false, "Allow merge when the branches have the same content")
	withCommitFlags(mergeCmd, true)
	rootCmd.AddCommand(mergeCmd)
}
