package cmd

import (
	"context"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show <repository uri>",
	Short: "See detailed information about an entity by ID (commit, user, etc)",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(1),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		u := uri.Must(uri.Parse(args[0]))
		oneOf := []string{"commit"}
		var found bool
		var showType, identifier string
		for _, flagName := range oneOf {
			value, err := cmd.Flags().GetString(flagName)
			if err != nil {
				continue
			}
			if len(value) > 0 {
				if found {
					DieFmt("please specify one of \"%s\"", strings.Join(oneOf, ", "))
				}
				found = true
				showType = flagName
				identifier = value
			}
		}

		switch showType {
		case "commit":
			client := getClient()
			commit, err := client.GetCommit(context.Background(), u.Repository, identifier)
			if err != nil {
				DieErr(err)
			}
			showMetaRangeID, _ := cmd.Flags().GetBool("show-meta-range-id")
			commits := struct {
				Commits         []*models.Commit
				Pagination      *Pagination
				ShowMetaRangeID bool
			}{
				Commits:         []*models.Commit{commit},
				ShowMetaRangeID: showMetaRangeID,
			}
			Write(commitsTemplate, commits)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(showCmd)

	showCmd.Flags().String("commit", "", "commit ID to show")
	showCmd.Flags().Bool("show-meta-range-id", false, "when showing commits, also show meta range ID")
}
