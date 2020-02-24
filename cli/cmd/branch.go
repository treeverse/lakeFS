package cmd

import (
	"context"
	"strings"

	"github.com/go-openapi/swag"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/treeverse/lakefs/uri"

	"github.com/spf13/cobra"
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
	Use:     "list [repository uri]",
	Short:   "list branches in a repository",
	Example: "lakectl branch list lakefs://myrepo",
	Args: ValidationChain(
		HasNArgs(1),
		IsRepoURI(0),
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
			rows[i] = []interface{}{*row.ID, *row.CommitID}
		}

		ctx := struct {
			BranchTable *Table
			Pagination  *Pagination
		}{
			BranchTable: &Table{
				Headers: []interface{}{"Branch Name", "Commit ID"},
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
	Use:   "create [branch uri]",
	Short: "create a new branch in a repository",
	Args: ValidationChain(
		HasNArgs(1),
		IsBranchURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		u := uri.Must(uri.Parse(args[0]))
		client := getClient()
		sourceRawUri, _ := cmd.Flags().GetString("source")
		sourceURI, err := uri.Parse(sourceRawUri)
		if err != nil {
			DieFmt("failed to parse source URI: %s", err)
		}
		if !strings.EqualFold(sourceURI.Repository, u.Repository) {
			DieFmt("source branch must be in the same repository")
		}

		sourceBranch, err := client.GetBranch(context.Background(), u.Repository, sourceURI.Refspec)
		if err != nil {
			DieFmt("could not get source branch: %s", err)
		}
		Fmt("got source branch '%s', using commit '%s'\n",
			sourceURI.Refspec, *sourceBranch.CommitID)
		err = client.CreateBranch(context.Background(), u.Repository, &models.Refspec{
			CommitID: sourceBranch.CommitID,
			ID:       swag.String(u.Refspec),
		})
		if err != nil {
			DieErr(err)
		}
	},
}

var branchDeleteCmd = &cobra.Command{
	Use:   "delete [branch uri]",
	Short: "delete a branch in a repository, along with its uncommitted changes (CAREFUL)",
	Args: ValidationChain(
		HasNArgs(1),
		IsBranchURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		sure, err := cmd.Flags().GetBool("sure")
		if err != nil || !sure {
			confirmation, err := confirm("Are you sure you want to delete branch")
			if err != nil || !confirmation {
				DieFmt("please confirm by passing the --sure | -y flag")
			}
		}
		client := getClient()
		u := uri.Must(uri.Parse(args[0]))
		err = client.DeleteBranch(context.Background(), u.Repository, u.Refspec)
		if err != nil {
			DieErr(err)
		}
	},
}

func moreThanOne(args ...bool) bool {
	count := 0
	for _, args := range args {
		if args {
			count++
		}
	}
	return count > 1
}

// lakectl branch revert lakefs://myrepo@master --commit commitId --tree path --object path
var branchRevertCmd = &cobra.Command{
	Use:   "revert [branch uri] [flags]",
	Short: "revert changes to specified commit, or revert uncommitted changes - all changes, or by path ",
	Long: `revert changes: there are four different ways to revert changes:
				1. revert to previous commit, set HEAD of branch to given commit -  revert lakefs://myrepo@master --commit commitId
				2. revert all uncommitted changes (reset) -  revert lakefs://myrepo@master 
				3. revert uncommitted changes under specific path -	revert lakefs://myrepo@master  --tree path
				4. revert uncommited changes for specific object  - revert lakefs://myrepo@master  --object path `,
	Args: ValidationChain(
		HasNArgs(1),
		IsBranchURI(0),
	),
	RunE: func(cmd *cobra.Command, args []string) error {
		clt, err := getClient()
		if err != nil {
			return err
		}
		u := uri.Must(uri.Parse(args[0]))

		commitId, err := cmd.Flags().GetString("commit")
		if err != nil {
			return err
		}
		tree, err := cmd.Flags().GetString("tree")
		if err != nil {
			return err
		}
		object, err := cmd.Flags().GetString("object")
		if err != nil {
			return err
		}
		isCommit := len(commitId) > 0
		isTree := len(tree) > 0
		isObject := len(object) > 0

		if moreThanOne(isCommit, isTree, isObject) {
			return xerrors.Errorf("can't revert by multiple commands, please choose only one [commit, tree, object]!")
		}

		var revert models.RevertCreation
		var confirmationMsg string
		if isCommit {
			confirmationMsg = fmt.Sprintf("Are you sure you want to revert all changes to commit: %s ?", commitId)
			revert = models.RevertCreation{
				Commit: commitId,
				Type:   swag.String(models.RevertCreationTypeCOMMIT),
			}
		} else if isTree {
			confirmationMsg = fmt.Sprintf("Are you sure you want to revert all changes from path: %s to last commit?", tree)
			revert = models.RevertCreation{
				Path: tree,
				Type: swag.String(models.RevertCreationTypeTREE),
			}
		} else if isObject {
			confirmationMsg = fmt.Sprintf("Are you sure you want to revert all changes for object: %s to last commit?", object)
			revert = models.RevertCreation{
				Path: object,
				Type: swag.String(models.RevertCreationTypeOBJECT),
			}
		} else {
			confirmationMsg = "are you sure you want to revert all uncommitted changes?"
			revert = models.RevertCreation{
				Type: swag.String(models.RevertCreationTypeRESET),
			}
		}

		confirmation, err := confirm(confirmationMsg)
		if err != nil || !confirmation {
			fmt.Println("Revert Aborted")
			return nil
		}
		err = clt.RevertBranch(context.Background(), u.Repository, u.Refspec, &revert)
		return err
	},
}

var branchShowCmd = &cobra.Command{
	Use:   "show [branch uri]",
	Short: "show branch metadata",
	Args: ValidationChain(
		HasNArgs(1),
		IsBranchURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := uri.Must(uri.Parse(args[0]))
		resp, err := client.GetBranch(context.Background(), u.Repository, u.Refspec)
		if err != nil {
			DieErr(err)
		}
		Fmt("%s\t%s\n", *resp.ID, *resp.CommitID)
	},
}

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

	branchDeleteCmd.Flags().BoolP("sure", "y", false, "do not ask for confirmation")

	branchRevertCmd.Flags().StringP("commit", "c", "", "commit ID to revert branch to ")
	branchRevertCmd.Flags().StringP("tree", "p", "", "path to tree to be reverted")
	branchRevertCmd.Flags().StringP("object", "o", "", "path to object to be reverted")
}
