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
	uriPrefix := uri.LakeFSSchema + uri.LakeFSSchemaSeparator
	if strings.HasPrefix(toComplete, uriPrefix) && strings.Contains(toComplete[len(uriPrefix):], uri.PathSeparator) {
		return nil, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	}

	// extract repository name written so far
	var prefix api.PaginationPrefix
	if strings.HasPrefix(toComplete, uriPrefix) {
		if !strings.Contains(toComplete[len(uriPrefix):], uri.PathSeparator) {
			prefix = api.PaginationPrefix(toComplete[len(uriPrefix):])
		}
	}

	// suggest repositories
	clt := getClient()
	var (
		completions []string
		after       string
	)
	for {
		params := &api.ListRepositoriesParams{
			Prefix: &prefix,
			After:  api.PaginationAfterPtr(after),
		}
		resp, err := clt.ListRepositoriesWithResponse(ctx, params)
		result := resp.JSON200
		if err != nil || result == nil {
			return nil, cobra.ShellCompDirectiveError
		}
		for _, repo := range result.Results {
			completions = append(completions, uriPrefix+repo.Id)
		}
		if !result.Pagination.HasMore {
			break
		}
		after = result.Pagination.NextOffset
	}
	return completions, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
}
