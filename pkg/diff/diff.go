package diff

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/uri"
)

const diffTypeTwoDot = "two_dot"

// StreamRepositoryDiffs asynchronously fetches differences between 'left' and 'right' references, assumes both are in the same repository
func StreamRepositoryDiffs(ctx context.Context, client apigen.ClientWithResponsesInterface, left, right *uri.URI, prefix string, diffs chan<- apigen.Diff, twoDot bool) error {
	defer func() {
		close(diffs)
	}()
	var diffType *string
	if twoDot {
		diffType = apiutil.Ptr(diffTypeTwoDot)
	}

	hasMore := true
	var after string
	for hasMore {
		diffResp, err := client.DiffRefsWithResponse(ctx, left.Repository, left.Ref, right.Ref, &apigen.DiffRefsParams{
			After:  (*apigen.PaginationAfter)(swag.String(after)),
			Prefix: (*apigen.PaginationPrefix)(&prefix),
			Type:   diffType,
		})
		if err != nil {
			return err
		}
		if diffResp.HTTPResponse.StatusCode != http.StatusOK {
			return fmt.Errorf("diff remote failed. HTTP %d: %w", diffResp.StatusCode(), local.ErrRemoteFailure)
		}

		for _, d := range diffResp.JSON200.Results {
			diffs <- d
		}

		hasMore = diffResp.JSON200.Pagination.HasMore
		after = diffResp.JSON200.Pagination.NextOffset
	}
	return nil
}

func Fmt(change string) (string, text.Color) {
	var color text.Color
	var action string

	switch change {
	case "added":
		color = text.FgGreen
		action = "+ added"
	case "removed":
		color = text.FgRed
		action = "- removed"
	case "changed", "modified":
		color = text.FgYellow
		action = "~ modified"
	case "conflict":
		color = text.FgHiYellow
		action = "* conflict"
	default:
	}
	return action, color
}
