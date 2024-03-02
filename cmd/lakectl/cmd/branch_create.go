package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
)

var branchCreateCmd = &cobra.Command{
	Use:               "create <branch URI> -s <source ref URI>",
	Short:             "Create a new branch in a repository",
	Example:           "lakectl branch create lakefs://example-repo/new-branch -s lakefs://example-repo/main",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseBranchURI("branch URI", args[0])
		client := getClient()
		sourceRawURI := Must(cmd.Flags().GetString("source"))
		sourceURI, err := uri.ParseWithBaseURI(sourceRawURI, baseURI)
		if err != nil {
			DieFmt("failed to parse source URI: %s", err)
		}
		fmt.Println("Source ref:", sourceURI)
		if sourceURI.Repository != u.Repository {
			Die("source branch must be in the same repository", 1)
		}

		resp, err := client.CreateBranchWithResponse(cmd.Context(), u.Repository, apigen.CreateBranchJSONRequestBody{
			Name:   u.Ref,
			Source: sourceURI.Ref,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		fmt.Printf("created branch '%s' %s\n", u.Ref, string(resp.Body))
	},
}

//nolint:gochecknoinits
func init() {
	branchCreateCmd.Flags().StringP("source", "s", "", "source branch uri")
	_ = branchCreateCmd.MarkFlagRequired("source")
	_ = branchCreateCmd.RegisterFlagCompletionFunc("source", ValidArgsRepository)

	branchCmd.AddCommand(branchCreateCmd)
}
