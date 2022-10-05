package cmd

import (
	"context"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

func ValidArgsRepository(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return validRepositoryToComplete(cmd.Context(), toComplete)
}

func validRepositoryToComplete(ctx context.Context, toComplete string) ([]string, cobra.ShellCompDirective) {
	// do not suggest in case we are passed the repository part
	prefix := uri.LakeFSSchema + uri.LakeFSSchemaSeparator
	if strings.HasPrefix(toComplete, prefix) && strings.Contains(toComplete[len(prefix):], uri.PathSeparator) {
		return nil, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	}

	// suggest repositories
	// TODO(barak): smart suggest based on current value
	clt := getClient()
	resp, err := clt.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
	if err != nil || resp.JSON200 == nil {
		return nil, cobra.ShellCompDirectiveError
	}
	results := make([]string, 0, len(resp.JSON200.Results))
	for _, repo := range resp.JSON200.Results {
		results = append(results, prefix+repo.Id)
	}
	return results, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
}
