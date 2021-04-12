package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/uri"
)

const tagListTemplate = `{{.TagTable | table -}}
{{.Pagination | paginate }}
`

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
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		var pagination api.Pagination
		rows := make([][]interface{}, 0)

		u := uri.Must(uri.Parse(args[0]))
		ctx := cmd.Context()
		client := getClient()
		for {
			amountForPagination := amount
			if amountForPagination == -1 {
				amountForPagination = defaultPaginationAmount
			}
			resp, err := client.ListTagsWithResponse(ctx, u.Repository, &api.ListTagsParams{
				After:  api.PaginationAfterPtr(after),
				Amount: api.PaginationAmountPtr(amountForPagination),
			})
			DieOnResponseError(resp, err)

			refs := resp.JSON200.Results
			for _, row := range refs {
				rows = append(rows, []interface{}{row.Id, row.CommitId})
			}
			pagination = resp.JSON200.Pagination
			if amount != -1 || !pagination.HasMore {
				break
			}
			after = pagination.NextOffset
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
		if pagination.HasMore {
			tmplArgs.Pagination = &Pagination{
				Amount:  amount,
				HasNext: true,
				After:   pagination.NextOffset,
			}
		}
		Write(tagListTemplate, tmplArgs)
	},
}

var tagCreateCmd = &cobra.Command{
	Use:   "create <tag uri> <commit ref>",
	Short: "create a new tag in a repository",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(tagCreateRequiredArgs),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		tagURI := uri.Must(uri.Parse(args[0]))
		client := getClient()
		commitRef := args[1]
		ctx := cmd.Context()
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
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete tag")
		if err != nil || !confirmation {
			Die("Delete tag aborted", 1)
		}
		client := getClient()
		u := uri.Must(uri.Parse(args[0]))
		ctx := cmd.Context()
		resp, err := client.DeleteTagWithResponse(ctx, u.Repository, u.Ref)
		DieOnResponseError(resp, err)
	},
}

var tagShowCmd = &cobra.Command{
	Use:   "show <tag uri>",
	Short: "show tag's commit reference",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := uri.Must(uri.Parse(args[0]))
		ctx := cmd.Context()
		resp, err := client.GetTagWithResponse(ctx, u.Repository, u.Ref)
		DieOnResponseError(resp, err)
		Fmt("%s %s", resp.JSON200.Id, resp.JSON200.CommitId)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(tagCmd)
	tagCmd.AddCommand(tagCreateCmd, tagDeleteCmd, tagListCmd, tagShowCmd)
	flags := tagListCmd.Flags()
	flags.Int("amount", -1, "number of results to return. By default, all results are returned.")
	flags.String("after", "", "show results after this value (used for pagination)")
}
