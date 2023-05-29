package local

import (
	"context"
	"fmt"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
	"net/http"
	"strings"
)

type Client interface {
	api.ClientInterface
	api.ClientWithResponsesInterface
}

func DereferenceURI(ctx context.Context, client Client, remote *uri.URI) (head string, err error) {
	response, err := client.GetCommitWithResponse(ctx, remote.Repository, remote.Ref)
	if err != nil {
		return
	}
	if response.StatusCode() == http.StatusNotFound {
		return head, ErrNoCommitFound
	} else if response.StatusCode() != http.StatusOK {
		return head, fmt.Errorf("%w: GetCommit: HTTP %d", ErrLakeFSError, response.StatusCode())
	}

	return response.JSON200.Id, nil
}

func WithRef(remote *uri.URI, ref string) *uri.URI {
	return &uri.URI{
		Repository: remote.Repository,
		Ref:        ref,
		Path:       remote.Path,
	}
}

func ListRemote(ctx context.Context, client api.ClientWithResponsesInterface, loc *uri.URI) ([]api.ObjectStats, error) {
	hasMore := true
	objects := make([]api.ObjectStats, 0)
	var after string
	for hasMore {
		listResp, err := client.ListObjectsWithResponse(ctx, loc.Repository, loc.Ref, &api.ListObjectsParams{
			After:        (*api.PaginationAfter)(swag.String(after)),
			Prefix:       (*api.PaginationPrefix)(loc.Path),
			UserMetadata: swag.Bool(true),
		})
		if err != nil {
			return nil, err
		}
		if listResp.HTTPResponse.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%w: HTTP %d", ErrRemoteDiffFailed, listResp.StatusCode())
		}
		for _, o := range listResp.JSON200.Results {
			path := strings.TrimPrefix(o.Path, loc.GetPath())
			// skip directory markers
			if path == "" || (strings.HasSuffix(path, PathSeparator) && swag.Int64Value(o.SizeBytes) == 0) {
				continue
			}
			path = strings.TrimPrefix(path, PathSeparator)
			objects = append(objects, api.ObjectStats{
				Checksum:        o.Checksum,
				ContentType:     o.ContentType,
				Metadata:        o.Metadata,
				Mtime:           o.Mtime,
				Path:            path,
				PathType:        o.PathType,
				PhysicalAddress: o.PhysicalAddress,
				SizeBytes:       o.SizeBytes,
			})
		}
		hasMore = listResp.JSON200.Pagination.HasMore
		after = listResp.JSON200.Pagination.NextOffset
	}
	return objects, nil
}
