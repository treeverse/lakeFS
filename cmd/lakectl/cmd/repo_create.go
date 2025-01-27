package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	DefaultBranch = "main"

	repoCreateCmdArgs = 2
)

// repoCreateCmd represents the create repo command
var repoCreateCmd = &cobra.Command{
	Use:               "create <repository URI> <storage namespace> [--storage-id <storage id>]",
	Short:             "Create a new repository",
	Example:           "lakectl repo create " + myRepoExample + " " + myBucketExample,
	Args:              cobra.ExactArgs(repoCreateCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		fmt.Println("Repository:", u)

		defaultBranch, err := cmd.Flags().GetString("default-branch")
		if err != nil {
			DieErr(err)
		}
		storageID, _ := cmd.Flags().GetString("storage-id")

		resp, err := clt.CreateRepositoryWithResponse(cmd.Context(),
			&apigen.CreateRepositoryParams{},
			apigen.CreateRepositoryJSONRequestBody{
				Name:             u.Repository,
				StorageId:        &storageID,
				StorageNamespace: args[1],
				DefaultBranch:    &defaultBranch,
			})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}
		repo := resp.JSON201
		fmt.Printf("Repository '%s' created:\nstorage namespace: %s\ndefault branch: %s\ntimestamp: %d\n", repo.Id, repo.StorageNamespace, repo.DefaultBranch, repo.CreationDate)
	},
}

//nolint:gochecknoinits
func init() {
	repoCreateCmd.Flags().StringP("default-branch", "d", DefaultBranch, "the default branch of this repository")
	repoCreateCmd.Flags().String("storage-id", "", "the storage of this repository")

	repoCmd.AddCommand(repoCreateCmd)
}
