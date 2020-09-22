package cmd

import (
	"context"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/uri"
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show <repository uri>",
	Short: "See detailed information about an entity by ID (commit, user, etc)",
	Args: ValidationChain(
		HasNArgs(1),
		PositionValidator(0, uri.ValidateRepoURI),
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
			commits := struct {
				Commits    []*models.Commit
				Pagination *Pagination
			}{
				Commits: []*models.Commit{commit},
			}
			Write(commitsTemplate, commits)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(showCmd)

	showCmd.Flags().String("commit", "c", "commit id to show")
}
