package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	DefaultBranch = "main"

	repoCreateCmdArgs = 2
)

// repoCreateCmd represents the create repo command
// lakectl create lakefs://myrepo s3://my-bucket/
var repoCreateCmd = &cobra.Command{
	Use:               "create <repository uri> <storage namespace>",
	Short:             "Create a new repository",
	Example:           "lakectl repo create lakefs://some-repo-name s3://some-bucket-name",
	Args:              cobra.ExactArgs(repoCreateCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRepoURI("repository", args[0])
		fmt.Printf("Repository: %s\n", []interface{}{u.String()}...)
		defaultBranch, err := cmd.Flags().GetString("default-branch")
		if err != nil {
			DieErr(err)
		}
		resp, err := clt.CreateRepositoryWithResponse(cmd.Context(),
			&api.CreateRepositoryParams{},
			api.CreateRepositoryJSONRequestBody{
				Name:             u.Repository,
				StorageNamespace: args[1],
				DefaultBranch:    &defaultBranch,
			})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}
		repo := resp.JSON201
		fmt.Printf("Repository '%s' created:\nstorage namespace: %s\ndefault branch: %s\ntimestamp: %d\n", []interface{}{repo.Id, repo.StorageNamespace, repo.DefaultBranch, repo.CreationDate}...)
	},
}

func init() {
	repoCreateCmd.Flags().StringP("default-branch", "d", DefaultBranch, "the default branch of this repository")

	repoCmd.AddCommand(repoCreateCmd)
}
