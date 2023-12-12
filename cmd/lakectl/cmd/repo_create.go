package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	DefaultBranch = "main"

	repoCreateCmdArgs = 3
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
		defaultBranch, err := cmd.Flags().GetString("default-branch")
		if err != nil {
			DieErr(err)
		}
		readonlyRepository, err := cmd.Flags().GetBool("read-only")
		if err != nil {
			DieErr(err)
		}
		resp, err := clt.CreateRepositoryWithResponse(cmd.Context(),
			&apigen.CreateRepositoryParams{},
			apigen.CreateRepositoryJSONRequestBody{
				Name:             u.Repository,
				StorageNamespace: args[1],
				DefaultBranch:    &defaultBranch,
				ReadOnly:         &readonlyRepository,
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
	repoCreateCmd.Flags().BoolP("read-only", "r", false, "whether the repository is read-only")
	repoCmd.AddCommand(repoCreateCmd)
}
