package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	DefaultBranch = "main"

	SampleDataFlag = "sample-data"

	repoCreateCmdArgs = 2
)

// repoCreateCmd represents the create repo command
var repoCreateCmd = &cobra.Command{
	Use:               "create <repository URI> <storage namespace>",
	Short:             "Create a new repository",
	Example:           "lakectl repo create " + myRepoExample + " " + myBucketExample,
	Args:              cobra.ExactArgs(repoCreateCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		fmt.Println("Repository:", u)
		defaultBranch := Must(cmd.Flags().GetString("default-branch"))
		sampleData := Must(cmd.Flags().GetBool(SampleDataFlag))

		resp, err := clt.CreateRepositoryWithResponse(cmd.Context(),
			&apigen.CreateRepositoryParams{},
			apigen.CreateRepositoryJSONRequestBody{
				Name:             u.Repository,
				StorageNamespace: args[1],
				DefaultBranch:    &defaultBranch,
				SampleData:       &sampleData,
			})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}
		repo := resp.JSON201
		fmt.Printf("Repository '%s' created:\nstorage namespace: %s\ndefault branch: %s\ntimestamp: %d\n", repo.Id, repo.StorageNamespace, repo.DefaultBranch, repo.CreationDate)
		if sampleData {
			fmt.Println("sample data included")
		}
	},
}

//nolint:gochecknoinits
func init() {
	repoCreateCmd.Flags().StringP("default-branch", "d", DefaultBranch, "the default branch of this repository")
	repoCreateCmd.Flags().Bool(SampleDataFlag, false, "create sample data in the repository")

	repoCmd.AddCommand(repoCreateCmd)
}
