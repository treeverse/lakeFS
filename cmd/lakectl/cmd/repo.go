package cmd

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	DefaultBranch     = "main"
	repoCreateCmdArgs = 2
)

// repoCmd represents the repo command
var repoCmd = &cobra.Command{
	Use:   "repo",
	Short: "Manage and explore repos",
}

var repoListCmd = &cobra.Command{
	Use:   "list",
	Short: "List repositories",
	Args:  cobra.NoArgs,
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return nil, cobra.ShellCompDirectiveNoFileComp
	},
	Run: func(cmd *cobra.Command, args []string) {
		amount := utils.MustInt(cmd.Flags().GetInt("amount"))
		after := utils.MustString(cmd.Flags().GetString("after"))
		clt := getClient()

		resp, err := clt.ListRepositoriesWithResponse(cmd.Context(), &api.ListRepositoriesParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			utils.Die("Bad response from server", 1)
		}
		repos := resp.JSON200.Results
		rows := make([][]interface{}, len(repos))
		for i, repo := range repos {
			ts := time.Unix(repo.CreationDate, 0).String()
			rows[i] = []interface{}{repo.Id, ts, repo.DefaultBranch, repo.StorageNamespace}
		}
		pagination := resp.JSON200.Pagination
		utils.PrintTable(rows, []interface{}{"Repository", "Creation Date", "Default Ref Name", "Storage Namespace"}, &pagination, amount)
	},
}

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
		u := utils.MustParseRepoURI("repository", args[0])
		utils.Fmt("Repository: %s\n", u.String())
		defaultBranch, err := cmd.Flags().GetString("default-branch")
		if err != nil {
			utils.DieErr(err)
		}
		resp, err := clt.CreateRepositoryWithResponse(cmd.Context(),
			&api.CreateRepositoryParams{},
			api.CreateRepositoryJSONRequestBody{
				Name:             u.Repository,
				StorageNamespace: args[1],
				DefaultBranch:    &defaultBranch,
			})
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			utils.Die("Bad response from server", 1)
		}
		repo := resp.JSON201
		utils.Fmt("Repository '%s' created:\nstorage namespace: %s\ndefault branch: %s\ntimestamp: %d\n",
			repo.Id, repo.StorageNamespace, repo.DefaultBranch, repo.CreationDate)
	},
}

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
		u := utils.MustParseRepoURI("repository", args[0])
		utils.Fmt("Repository: %s\n", u.String())
		defaultBranch, err := cmd.Flags().GetString("default-branch")
		if err != nil {
			utils.DieErr(err)
		}
		bareRepo := true
		resp, err := clt.CreateRepositoryWithResponse(cmd.Context(), &api.CreateRepositoryParams{
			Bare: &bareRepo,
		}, api.CreateRepositoryJSONRequestBody{
			DefaultBranch:    &defaultBranch,
			Name:             u.Repository,
			StorageNamespace: args[1],
		})
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			utils.Die("Bad response from server", 1)
		}
		repo := resp.JSON201
		utils.Fmt("Repository '%s' created:\nstorage namespace: %s\ndefault branch: %s\ntimestamp: %d\n",
			repo.Id, repo.StorageNamespace, repo.DefaultBranch, repo.CreationDate)
	},
}

// repoDeleteCmd represents the delete repo command
// lakectl delete lakefs://myrepo
var repoDeleteCmd = &cobra.Command{
	Use:               "delete <repository uri>",
	Short:             "Delete existing repository",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := utils.MustParseRepoURI("repository", args[0])
		utils.Fmt("Repository: %s\n", u.String())
		confirmation, err := utils.Confirm(cmd.Flags(), "Are you sure you want to delete repository: "+u.Repository)
		if err != nil || !confirmation {
			utils.DieFmt("Delete Repository '%s' aborted\n", u.Repository)
		}
		resp, err := clt.DeleteRepositoryWithResponse(cmd.Context(), u.Repository)
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		utils.Fmt("Repository '%s' deleted\n", u.Repository)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(repoCmd)
	repoCmd.AddCommand(repoListCmd)
	repoCmd.AddCommand(repoCreateCmd)
	repoCmd.AddCommand(repoCreateBareCmd)
	repoCmd.AddCommand(repoDeleteCmd)

	repoListCmd.Flags().Int("amount", utils.DefaultAmountArgumentValue, "number of results to return")
	repoListCmd.Flags().String("after", "", "show results after this value (used for pagination)")

	repoCreateCmd.Flags().StringP("default-branch", "d", DefaultBranch, "the default branch of this repository")

	repoCreateBareCmd.Flags().StringP("default-branch", "d", DefaultBranch, "the default branch name of this repository (will not be created)")

	utils.AssignAutoConfirmFlag(repoDeleteCmd.Flags())
}
