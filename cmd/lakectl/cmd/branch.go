package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

const branchRevertCmdArgs = 2

const (
	ParentNumberFlagName = "parent-number"
)

// branchCmd represents the branch command
var branchCmd = &cobra.Command{
	Use:   "branch",
	Short: "create and manage branches within a repository",
	Long:  `Create delete and list branches within a lakeFS repository`,
}

const branchListTemplate = `{{.BranchTable | table -}}
{{.Pagination | paginate }}
`

var branchListCmd = &cobra.Command{
	Use:     "list <repository uri>",
	Short:   "list branches in a repository",
	Example: "lakectl branch list lakefs://<repository>",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		u := MustParseRepoURI("repository", args[0])
		client := getClient()
		resp, err := client.ListBranchesWithResponse(cmd.Context(), u.Repository, &api.ListBranchesParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(resp, err)

		refs := resp.JSON200.Results
		rows := make([][]interface{}, len(refs))
		for i, row := range refs {
			rows[i] = []interface{}{row.Id, row.CommitId}
		}

		pagination := resp.JSON200.Pagination
		data := struct {
			BranchTable *Table
			Pagination  *Pagination
		}{
			BranchTable: &Table{
				Headers: []interface{}{"Branch", "Commit ID"},
				Rows:    rows,
			},
		}
		if pagination.HasMore {
			data.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}

		Write(branchListTemplate, data)
	},
}

var branchCreateCmd = &cobra.Command{
	Use:   "create <ref uri>",
	Short: "create a new branch in a repository",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("branch", args[0])
		client := getClient()
		sourceRawURI, _ := cmd.Flags().GetString("source")
		sourceURI, err := uri.ParseWithBaseURI(sourceRawURI, baseURI)
		if err != nil {
			DieFmt("failed to parse source URI: %s", err)
		}
		Fmt("Source ref: %s\n", sourceURI.String())
		if sourceURI.Repository != u.Repository {
			Die("source branch must be in the same repository", 1)
		}

		resp, err := client.CreateBranchWithResponse(cmd.Context(), u.Repository, api.CreateBranchJSONRequestBody{
			Name:   u.Ref,
			Source: sourceURI.Ref,
		})
		DieOnResponseError(resp, err)
		Fmt("created branch '%s'\n", string(resp.Body))
	},
}

var branchDeleteCmd = &cobra.Command{
	Use:   "delete <branch uri>",
	Short: "delete a branch in a repository, along with its uncommitted changes (CAREFUL)",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete branch")
		if err != nil || !confirmation {
			Die("Delete branch aborted", 1)
		}
		client := getClient()
		u := MustParseRefURI("branch", args[0])
		Fmt("Branch: %s\n", u.String())
		resp, err := client.DeleteBranchWithResponse(cmd.Context(), u.Repository, u.Ref)
		DieOnResponseError(resp, err)
	},
}

// lakectl branch revert lakefs://myrepo/master commitId
var branchRevertCmd = &cobra.Command{
	Use:   "revert <branch uri> <commit ref to revert>",
	Short: "given a commit, record a new commit to reverse the effect of this commit",
	Args:  cobra.ExactArgs(branchRevertCmdArgs),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRefURI("branch", args[0])
		Fmt("Branch: %s\n", u.String())
		commitRef := args[1]
		clt := getClient()
		hasParentNumber := cmd.Flags().Changed(ParentNumberFlagName)
		parentNumber, _ := cmd.Flags().GetInt(ParentNumberFlagName)
		if hasParentNumber && parentNumber <= 0 {
			Die("parent number must be non-negative, if specified", 1)
		}
		confirmation, err := Confirm(cmd.Flags(), fmt.Sprintf("Are you sure you want to revert the effect of commit %s", commitRef))
		if err != nil || !confirmation {
			Die("Revert aborted", 1)
		}
		resp, err := clt.RevertBranchWithResponse(cmd.Context(), u.Repository, u.Ref, api.RevertBranchJSONRequestBody{
			ParentNumber: parentNumber,
			Ref:          commitRef,
		})
		DieOnResponseError(resp, err)
	},
}

// lakectl branch reset lakefs://myrepo/master --commit commitId --prefix path --object path
var branchResetCmd = &cobra.Command{
	Use:   "reset <branch uri> [flags]",
	Short: "reset changes to specified commit, or reset uncommitted changes - all changes, or by path",
	Long: `reset changes.  There are four different ways to reset changes:
  1. reset to previous commit, set HEAD of branch to given commit - reset lakefs://myrepo/master --commit commitId
  2. reset all uncommitted changes - reset lakefs://myrepo/master 
  3. reset uncommitted changes under specific path -	reset lakefs://myrepo/master --prefix path
  4. reset uncommitted changes for specific object - reset lakefs://myrepo/master --object path`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRefURI("branch", args[0])
		Fmt("Branch: %s\n", u.String())
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

		var reset api.ResetCreation
		var confirmationMsg string
		switch {
		case len(commitID) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to reset all changes to commit: %s", commitID)
			reset = api.ResetCreation{
				Commit: &commitID,
				Type:   "commit",
			}
		case len(prefix) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to reset all changes from path: %s to last commit", prefix)
			reset = api.ResetCreation{
				Path: &prefix,
				Type: "common_prefix",
			}
		case len(object) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to reset all changes for object: %s to last commit", object)
			reset = api.ResetCreation{
				Path: &object,
				Type: "object",
			}
		default:
			confirmationMsg = "Are you sure you want to reset all uncommitted changes"
			reset = api.ResetCreation{
				Type: "reset",
			}
		}

		confirmation, err := Confirm(cmd.Flags(), confirmationMsg)
		if err != nil || !confirmation {
			Die("Reset aborted", 1)
			return
		}
		resp, err := clt.ResetBranchWithResponse(cmd.Context(), u.Repository, u.Ref, api.ResetBranchJSONRequestBody(reset))
		DieOnResponseError(resp, err)
	},
}

var branchShowCmd = &cobra.Command{
	Use:   "show <branch uri>",
	Short: "show branch latest commit reference",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRefURI("branch", args[0])
		Fmt("Branch: %s\n", u.String())
		resp, err := client.GetBranchWithResponse(cmd.Context(), u.Repository, u.Ref)
		DieOnResponseError(resp, err)
		branch := resp.JSON200
		Fmt("%s\n", branch)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(branchCmd)
	branchCmd.AddCommand(branchCreateCmd)
	branchCmd.AddCommand(branchDeleteCmd)
	branchCmd.AddCommand(branchListCmd)
	branchCmd.AddCommand(branchShowCmd)
	branchCmd.AddCommand(branchResetCmd)
	branchCmd.AddCommand(branchRevertCmd)

	branchListCmd.Flags().Int("amount", -1, "how many results to return, or '-1' for default (used for pagination)")
	branchListCmd.Flags().String("after", "", "show results after this value (used for pagination)")

	branchCreateCmd.Flags().StringP("source", "s", "", "source branch uri")
	_ = branchCreateCmd.MarkFlagRequired("source")

	branchResetCmd.Flags().String("commit", "", "commit ID to reset branch to")
	branchResetCmd.Flags().String("prefix", "", "prefix of the objects to be reset")
	branchResetCmd.Flags().String("object", "", "path to object to be reset")

	branchRevertCmd.Flags().IntP(ParentNumberFlagName, "m", 0, "the parent number (starting from 1) of the mainline. The revert will reverse the change relative to the specified parent.")

	AssignAutoConfirmFlag(branchResetCmd.Flags())
	AssignAutoConfirmFlag(branchRevertCmd.Flags())
	AssignAutoConfirmFlag(branchDeleteCmd.Flags())
}
