package cmd

import (
	"context"
	"fmt"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

// branchCmd represents the branch command
var branchCmd = &cobra.Command{
	Use:   "branch",
	Short: "create and manage branches within a repository",
	Long:  `Create delete and list branches within a lakeFS repository`,
}

var branchListTemplate = `{{.BranchTable | table -}}
{{.Pagination | paginate }}
`

var branchListCmd = &cobra.Command{
	Use:     "list <repository uri>",
	Short:   "list branches in a repository",
	Example: "lakectl branch list lakefs://<repository>",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		u := uri.Must(uri.Parse(args[0]))
		client := getClient()
		response, pagination, err := client.ListBranches(context.Background(), u.Repository, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(response))
		for i, row := range response {
			rows[i] = []interface{}{row}
		}

		ctx := struct {
			BranchTable *Table
			Pagination  *Pagination
		}{
			BranchTable: &Table{
				Headers: []interface{}{"Branch"},
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

		Write(branchListTemplate, ctx)
	},
}

var branchCreateCmd = &cobra.Command{
	Use:   "create <ref uri>",
	Short: "create a new branch in a repository",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		u := uri.Must(uri.Parse(args[0]))
		client := getClient()
		sourceRawURI, _ := cmd.Flags().GetString("source")
		sourceURI, err := uri.Parse(sourceRawURI)
		if err != nil {
			DieFmt("failed to parse source URI: %s", err)
		}
		if sourceURI.Repository != u.Repository {
			Die("source branch must be in the same repository", 1)
		}

		_, err = client.CreateBranch(context.Background(), u.Repository, &models.BranchCreation{
			Name:   swag.String(u.Ref),
			Source: swag.String(sourceURI.Ref),
		})
		if err != nil {
			DieErr(err)
		}

		Fmt("created branch '%s'\n", u.Ref)
	},
}

var branchDeleteCmd = &cobra.Command{
	Use:   "delete <branch uri>",
	Short: "delete a branch in a repository, along with its uncommitted changes (CAREFUL)",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		confirmation, err := confirm(cmd.Flags(), "Are you sure you want to delete branch")
		if err != nil || !confirmation {
			Die("Delete branch aborted", 1)
		}
		client := getClient()
		u := uri.Must(uri.Parse(args[0]))
		err = client.DeleteBranch(context.Background(), u.Repository, u.Ref)
		if err != nil {
			DieErr(err)
		}
	},
}

// lakectl branch revert lakefs://myrepo@master --commit commitId --prefix path --object path
var branchRevertCmd = &cobra.Command{
	Use:   "revert <branch uri> [flags]",
	Short: "revert changes to specified commit, or revert uncommitted changes - all changes, or by path",
	Long: `revert changes.  There are four different ways to revert changes:
  1. revert to previous commit, set HEAD of branch to given commit - revert lakefs://myrepo@master --commit commitId
  2. revert all uncommitted changes (reset) - revert lakefs://myrepo@master 
  3. revert uncommitted changes under specific path -	revert lakefs://myrepo@master --prefix path
  4. revert uncommitted changes for specific object - revert lakefs://myrepo@master --object path`,
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := uri.Must(uri.Parse(args[0]))

		commitID, err := cmd.Flags().GetString("commit")
		if err != nil {
			DieErr(err)
		}
		prefix, err := cmd.Flags().GetString("prefix")
		if err != nil {
			DieErr(err)
		}
		object, err := cmd.Flags().GetString("object")
		if err != nil {
			DieErr(err)
		}

		var revert models.RevertCreation
		var confirmationMsg string
		switch {
		case len(commitID) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to revert all changes to commit: %s", commitID)
			revert = models.RevertCreation{
				Commit: commitID,
				Type:   swag.String(models.RevertCreationTypeCommit),
			}
		case len(prefix) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to revert all changes from path: %s to last commit", prefix)
			revert = models.RevertCreation{
				Path: prefix,
				Type: swag.String(models.RevertCreationTypeCommonPrefix),
			}
		case len(object) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to revert all changes for object: %s to last commit", object)
			revert = models.RevertCreation{
				Path: object,
				Type: swag.String(models.RevertCreationTypeObject),
			}
		default:
			confirmationMsg = "Are you sure you want to revert all uncommitted changes"
			revert = models.RevertCreation{
				Type: swag.String(models.RevertCreationTypeReset),
			}
		}

		confirmation, err := confirm(cmd.Flags(), confirmationMsg)
		if err != nil || !confirmation {
			Die("Revert aborted", 1)
			return
		}
		err = clt.RevertBranch(context.Background(), u.Repository, u.Ref, &revert)
		if err != nil {
			DieErr(err)
		}
	},
}

var branchShowCmd = &cobra.Command{
	Use:   "show <branch uri>",
	Short: "show branch latest commit reference",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := uri.Must(uri.Parse(args[0]))
		ref, err := client.GetBranch(context.Background(), u.Repository, u.Ref)
		if err != nil {
			DieErr(err)
		}
		Fmt("%s\n", ref)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(branchCmd)
	branchCmd.AddCommand(branchCreateCmd)
	branchCmd.AddCommand(branchDeleteCmd)
	branchCmd.AddCommand(branchListCmd)
	branchCmd.AddCommand(branchShowCmd)
	branchCmd.AddCommand(branchRevertCmd)

	branchListCmd.Flags().Int("amount", -1, "how many results to return, or-1 for all results (used for pagination)")
	branchListCmd.Flags().String("after", "", "show results after this value (used for pagination)")

	branchCreateCmd.Flags().StringP("source", "s", "", "source branch uri")
	_ = branchCreateCmd.MarkFlagRequired("source")

	branchRevertCmd.Flags().String("commit", "", "commit ID to revert branch to")
	branchRevertCmd.Flags().String("prefix", "", "prefix of the objects to be reverted")
	branchRevertCmd.Flags().String("object", "", "path to object to be reverted")
}
