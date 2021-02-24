package cmd

import (
	"context"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
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

		u := uri.Must(uri.Parse(args[0]))
		ctx := context.Background()
		client := getClient()
		response, pagination, err := client.ListTags(ctx, u.Repository, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(response))
		for i, row := range response {
			rows[i] = []interface{}{swag.StringValue(row.ID), swag.StringValue(row.CommitID)}
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
		if pagination != nil && swag.BoolValue(pagination.HasMore) {
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
		ctx := context.Background()
		commitID, err := client.CreateTag(ctx, tagURI.Repository, tagURI.Ref, commitRef)
		if err != nil {
			DieErr(err)
		}
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
		ctx := context.Background()
		err = client.DeleteTag(ctx, u.Repository, u.Ref)
		if err != nil {
			DieErr(err)
		}
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
		ctx := context.Background()
		ref, err := client.GetTag(ctx, u.Repository, u.Ref)
		if err != nil {
			DieErr(err)
		}
		Fmt("%s\n", ref)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(tagCmd)
	tagCmd.AddCommand(tagCreateCmd, tagDeleteCmd, tagListCmd, tagShowCmd)
	flags := tagListCmd.Flags()
	flags.Int("amount", -1, "how many results to return, or '-1' for default (used for pagination)")
	flags.String("after", "", "show results after this value (used for pagination)")
}
