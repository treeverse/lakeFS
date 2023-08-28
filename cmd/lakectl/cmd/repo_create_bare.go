package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

// repoCreateBareCmd represents the create repo command
// lakectl create-bare lakefs://myrepo s3://my-bucket/
var repoCreateBareCmd = &cobra.Command{
	Use:               "create-bare <repository uri> <storage namespace>",
	Short:             "Create a new repository with no initial branch or commit",
	Example:           "lakectl create-bare lakefs://some-repo-name s3://some-bucket-name",
	Hidden:            true,
	Args:              cobra.ExactArgs(repoCreateCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRepoURI("repository", args[0])
		fmt.Println("Repository:", u)
		defaultBranch, err := cmd.Flags().GetString("default-branch")
		if err != nil {
			DieErr(err)
		}
		skipAccessibilityTest, err := cmd.Flags().GetBool("skip-accessibility-test")
		if err != nil {
			DieErr(err)
		}
		bareRepo := true
		resp, err := clt.CreateRepositoryWithResponse(cmd.Context(), &api.CreateRepositoryParams{
			Bare: &bareRepo,
		}, api.CreateRepositoryJSONRequestBody{
			DefaultBranch:         &defaultBranch,
			Name:                  u.Repository,
			StorageNamespace:      args[1],
			SkipAccessibilityTest: &skipAccessibilityTest,
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
	repoCreateBareCmd.Flags().StringP("default-branch", "d", DefaultBranch, "the default branch name of this repository (will not be created)")
	repoCreateBareCmd.Flags().Bool("skip-accessibility-test", false, "skip accessability check between lakeFS and the underlying storage")
	repoCmd.AddCommand(repoCreateBareCmd)
}
