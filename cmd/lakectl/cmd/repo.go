package cmd

import (
	"context"
	"time"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

const (
	DefaultBranch     = "master"
	repoCreateCmdArgs = 2
)

// repoCmd represents the repo command
var repoCmd = &cobra.Command{
	Use:   "repo",
	Short: "manage and explore repos",
}

var repoListTemplate = `{{.RepoTable | table -}}
{{.Pagination | paginate }}
`

var repoListCmd = &cobra.Command{
	Use:   "list",
	Short: "list repositories",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		repos, pagination, err := clt.ListRepositories(context.Background(), after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(repos))
		for i, repo := range repos {
			ts := time.Unix(repo.CreationDate, 0).String()
			rows[i] = []interface{}{repo.ID, ts, repo.DefaultBranch, repo.StorageNamespace}
		}

		ctx := struct {
			RepoTable  *Table
			Pagination *Pagination
		}{
			RepoTable: &Table{
				Headers: []interface{}{"Repository", "Creation Date", "Default Ref Name", "Storage Namespace"},
				Rows:    rows,
			},
		}
		if pagination != nil && swag.BoolValue(pagination.HasMore) {
			ctx.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}

		Write(repoListTemplate, ctx)
	},
}

// repoCreateCmd represents the create repo command
// lakectl create lakefs://myrepo s3://my-bucket/
var repoCreateCmd = &cobra.Command{
	Use:   "create <repository uri> <storage namespace>",
	Short: "create a new repository ",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(repoCreateCmdArgs),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),

	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := uri.Must(uri.Parse(args[0]))
		defaultBranch, err := cmd.Flags().GetString("default-branch")
		if err != nil {
			DieErr(err)
		}
		err = clt.CreateRepository(context.Background(), &models.RepositoryCreation{
			StorageNamespace: &args[1],
			DefaultBranch:    defaultBranch,
			Name:             &u.Repository,
		})
		if err != nil {
			DieErr(err)
		}
		repo, err := clt.GetRepository(context.Background(), u.Repository)
		if err != nil {
			DieErr(err)
		}
		Fmt("Repository '%s' created:\nstorage namespace: %s\ndefault branch: %s\ntimestamp: %d\n",
			repo.ID, repo.StorageNamespace, repo.DefaultBranch, repo.CreationDate)
	},
}

// repoCreateBareCmd represents the create repo command
// lakectl create-bare lakefs://myrepo s3://my-bucket/
var repoCreateBareCmd = &cobra.Command{
	Use:    "create-bare <repository uri> <storage namespace>",
	Short:  "create a new repository with no initial branch or commit",
	Hidden: true,
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(repoCreateCmdArgs),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),

	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := uri.Must(uri.Parse(args[0]))
		defaultBranch, err := cmd.Flags().GetString("default-branch")
		if err != nil {
			DieErr(err)
		}
		err = clt.CreateBareRepository(context.Background(), &models.RepositoryCreation{
			StorageNamespace: &args[1],
			DefaultBranch:    defaultBranch,
			Name:             &u.Repository,
		})
		if err != nil {
			DieErr(err)
		}
		repo, err := clt.GetRepository(context.Background(), u.Repository)
		if err != nil {
			DieErr(err)
		}
		Fmt("Repository '%s' created:\nstorage namespace: %s\ndefault branch: %s\ntimestamp: %d\n",
			repo.ID, repo.StorageNamespace, repo.DefaultBranch, repo.CreationDate)
	},
}

// repoDeleteCmd represents the delete repo command
// lakectl delete lakefs://myrepo
var repoDeleteCmd = &cobra.Command{
	Use:   "delete <repository uri>",
	Short: "delete existing repository",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := uri.Must(uri.Parse(args[0]))
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete repository: "+u.Repository)
		if err != nil || !confirmation {
			DieFmt("Delete Repository '%s' aborted\n", u.Repository)
		}
		err = clt.DeleteRepository(context.Background(), u.Repository)
		if err != nil {
			DieErr(err)
		}
		Fmt("Repository '%s' deleted\n", u.Repository)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(repoCmd)
	repoCmd.AddCommand(repoListCmd)
	repoCmd.AddCommand(repoCreateCmd)
	repoCmd.AddCommand(repoCreateBareCmd)
	repoCmd.AddCommand(repoDeleteCmd)

	repoListCmd.Flags().Int("amount", -1, "how many results to return, or '-1' for default (used for pagination)")
	repoListCmd.Flags().String("after", "", "show results after this value (used for pagination)")

	repoCreateCmd.Flags().StringP("default-branch", "d", DefaultBranch, "the default branch of this repository")

	repoCreateBareCmd.Flags().StringP("default-branch", "d", DefaultBranch, "the default branch name of this repository (will not be created)")

	AssignAutoConfirmFlag(repoDeleteCmd.Flags())
}
