package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const tagCreateRequiredArgs = 2

// tagCmd represents the tag command
var tagCmd = &cobra.Command{
	Use:   "tag",
	Short: "create and manage tags within a repository",
	Long:  `Create delete and list tags within a lakeFS repository`,
}

var tagListCmd = &cobra.Command{
	Use:     "list <repository uri>",
	Short:   "list tags in a repository",
	Example: "lakectl tag list lakefs://<repository>",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		amount := MustInt(cmd.Flags().GetInt("amount"))
		after := MustString(cmd.Flags().GetString("after"))

		u := MustParseRepoURI("repository", args[0])

		ctx := cmd.Context()
		client := getClient()
		resp, err := client.ListTagsWithResponse(ctx, u.Repository, &api.ListTagsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(resp, err)

		refs := resp.JSON200.Results
		rows := make([][]interface{}, len(refs))
		for i, row := range refs {
			rows[i] = []interface{}{row.Id, row.CommitId}
		}

		tmplArgs := struct {
			TagTable   *Table
			Pagination *Pagination
		}{
			TagTable: &Table{
				Headers: []interface{}{"Tag", "Commit ID"},
				Rows:    rows,
			},
		}
		pagination := resp.JSON200.Pagination
		if pagination.HasMore {
			tmplArgs.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}
		PrintTable(rows, []interface{}{"Tag", "Commit ID"}, &pagination, amount)
	},
}

var tagCreateCmd = &cobra.Command{
	Use:   "create <tag uri> <commit ref>",
	Short: "create a new tag in a repository",
	Args:  cobra.ExactArgs(tagCreateRequiredArgs),
	Run: func(cmd *cobra.Command, args []string) {
		tagURI := MustParseRefURI("tag", args[0])
		Fmt("Tag ref: %s\n", tagURI.String())

		client := getClient()
		commitRef := args[1]
		ctx := cmd.Context()
		force, _ := cmd.Flags().GetBool("force")
		if force {
			// checking validity of the commitRef before deleting the old one
			res, err := client.GetCommitWithResponse(ctx, tagURI.Repository, commitRef)
			DieOnResponseError(res, err)

			resp, err := client.DeleteTagWithResponse(ctx, tagURI.Repository, tagURI.Ref)
			if err != nil && (resp == nil || resp.JSON404 == nil) {
				DieOnResponseError(resp, err)
			}
		}

		resp, err := client.CreateTagWithResponse(ctx, tagURI.Repository, api.CreateTagJSONRequestBody{
			Id:  tagURI.Ref,
			Ref: commitRef,
		})
		DieOnResponseError(resp, err)

		commitID := *resp.JSON201
		Fmt("Created tag '%s' (%s)\n", tagURI.Ref, commitID)
	},
}

var tagDeleteCmd = &cobra.Command{
	Use:   "delete <tag uri>",
	Short: "delete a tag from a repository",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete tag")
		if err != nil || !confirmation {
			Die("Delete tag aborted", 1)
		}
		client := getClient()
		u := MustParseRefURI("tag", args[0])
		Fmt("Tag ref: %s\n", u.String())

		ctx := cmd.Context()
		resp, err := client.DeleteTagWithResponse(ctx, u.Repository, u.Ref)
		DieOnResponseError(resp, err)
	},
}

var tagShowCmd = &cobra.Command{
	Use:   "show <tag uri>",
	Short: "show tag's commit reference",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRefURI("tag", args[0])
		Fmt("Tag ref: %s\n", u.String())

		ctx := cmd.Context()
		resp, err := client.GetTagWithResponse(ctx, u.Repository, u.Ref)
		DieOnResponseError(resp, err)
		Fmt("%s %s", resp.JSON200.Id, resp.JSON200.CommitId)
	},
}

//nolint:gochecknoinits
func init() {
	tagCreateCmd.Flags().BoolP("force", "f", false, "override the tag if it exists")

	rootCmd.AddCommand(tagCmd)
	tagCmd.AddCommand(tagCreateCmd, tagDeleteCmd, tagListCmd, tagShowCmd)

	flags := tagListCmd.Flags()
	flags.Int("amount", defaultAmountArgumentValue, "number of results to return")
	flags.String("after", "", "show results after this value (used for pagination)")
}
