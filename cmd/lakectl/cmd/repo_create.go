package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	defaultBranchFlagName  = "default-branch"
	defaultBranchFlagValue = "main"
	sampleDataFlagName     = "sample-data"

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

		defaultBranch := Must(cmd.Flags().GetString(defaultBranchFlagName))
		sampleData := Must(cmd.Flags().GetBool(sampleDataFlagName))
		storageID := Must(cmd.Flags().GetString(storageIDFlagName))

		resp, err := clt.CreateRepositoryWithResponse(cmd.Context(),
			&apigen.CreateRepositoryParams{},
			apigen.CreateRepositoryJSONRequestBody{
				Name:             u.Repository,
				StorageId:        &storageID,
				StorageNamespace: args[1],
				DefaultBranch:    &defaultBranch,
				SampleData:       &sampleData,
			})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}
		repo := resp.JSON201
		fmt.Printf("Repository '%s' created:\nstorage namespace: %s\ndefault branch: %s\ntimestamp: %d\n",
			repo.Id, repo.StorageNamespace, repo.DefaultBranch, repo.CreationDate)
		if sampleData {
			fmt.Printf("sample data included\n")
		}
	},
}

//nolint:gochecknoinits
func init() {
	repoCreateCmd.Flags().StringP(defaultBranchFlagName, "d", defaultBranchFlagValue, "the default branch of this repository")
	repoCreateCmd.Flags().Bool(sampleDataFlagName, false, "create sample data in the repository")
	withStorageID(repoCreateCmd)

	repoCmd.AddCommand(repoCreateCmd)
}
