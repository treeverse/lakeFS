package local

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

type ChangeSource int

const (
	ChangeSourceRemote ChangeSource = iota
	ChangeSourceLocal
)

type ChangeType int

const (
	ChangeTypeAdded ChangeType = iota
	ChangeTypeModified
	ChangeTypeRemoved
	ChangeTypeConflict
)

type Change struct {
	Source ChangeSource
	Path   string
	Type   ChangeType
}

func (c *Change) String() string {
	return fmt.Sprintf("%s\t%s\t%s", ChangeSourceString(c.Source), ChangeTypeString(c.Type), c.Path)
}

func ChangeTypeFromString(changeType string) ChangeType {
	switch changeType {
	case "added":
		return ChangeTypeAdded
	case "removed":
		return ChangeTypeRemoved
	case "modified", "changed":
		return ChangeTypeModified
	case "conflict":
		return ChangeTypeConflict
	default:
		panic("invalid change type")
	}
}

func ChangeTypeString(changeType ChangeType) string {
	switch changeType {
	case ChangeTypeAdded:
		return "added"
	case ChangeTypeRemoved:
		return "removed"
	case ChangeTypeModified:
		return "modified"
	case ChangeTypeConflict:
		return "conflict"
	default:
		panic("invalid change type")
	}
}

func ChangeSourceString(changeSource ChangeSource) string {
	switch changeSource {
	case ChangeSourceLocal:
		return "local"
	case ChangeSourceRemote:
		return "remote"
	default:
		panic("invalid change source")
	}
}

type Changes []*Change

func (c Changes) String() string {
	strs := make([]string, len(c))
	for i, cc := range c {
		strs[i] = cc.String()
	}
	return strings.Join(strs, "\n")
}

// MergeWith combines changes from two diffs, sorting by lexicographic order.
// If the same path appears in both diffs, it's marked as a conflict.
func (c Changes) MergeWith(other Changes) Changes {
	cIdx := 0
	oIdx := 0
	result := make(Changes, 0)
	for cIdx < len(c) && oIdx < len(other) {
		switch {
		case c[cIdx].Path > other[oIdx].Path:
			// other is first
			result = append(result, other[oIdx])
			oIdx++
		case c[cIdx].Path < other[oIdx].Path:
			result = append(result, c[cIdx])
			cIdx++
		default: // both modified the same path!!

			result = append(result, &Change{
				Source: c[cIdx].Source,
				Path:   c[cIdx].Path,
				Type:   ChangeTypeConflict,
			})
			cIdx++
			oIdx++
		}
	}
	if cIdx < len(c) {
		result = append(result, c[cIdx:]...)
	}
	if oIdx < len(other) {
		result = append(result, other[oIdx:]...)
	}
	return result
}

// DiffLocalWithHead Checks changes between a local directory and the head it is pointing to. The diff check assumes the remote
// is an immutable set so any changes found resulted from changes in the local directory
// left is an object channel which contains results from a remote source. rightPath is the local directory to diff with
func DiffLocalWithHead(left <-chan api.ObjectStats, rightPath string) (Changes, error) {
	// left should be the base commit
	changes := make([]*Change, 0)
	var (
		currentRemoteFile api.ObjectStats
		hasMore           bool
	)
	err := filepath.Walk(rightPath, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() || diffShouldIgnore(info.Name()) {
			return nil
		}
		localPath := strings.TrimPrefix(path, rightPath)
		localPath = strings.TrimPrefix(localPath, string(filepath.Separator))
		localPath = filepath.ToSlash(localPath) // normalize to use "/" always

		localBytes := info.Size()
		localMtime := info.ModTime().Unix()
		for {
			if currentRemoteFile.Path == "" {
				if currentRemoteFile, hasMore = <-left; !hasMore {
					// nothing left on the left side, we definitely added stuff!
					changes = append(changes, &Change{ChangeSourceLocal, localPath, ChangeTypeAdded})
					break
				}
			}
			switch {
			case currentRemoteFile.Path < localPath: // We removed a file locally
				changes = append(changes, &Change{ChangeSourceLocal, currentRemoteFile.Path, ChangeTypeRemoved})
				currentRemoteFile.Path = ""
			case currentRemoteFile.Path == localPath:
				remoteMtime, err := getMtimeFromStats(currentRemoteFile)
				if err != nil {
					return err
				}
				if localBytes != swag.Int64Value(currentRemoteFile.SizeBytes) || localMtime != remoteMtime {
					// we made a change!
					changes = append(changes, &Change{ChangeSourceLocal, localPath, ChangeTypeModified})
				}
				currentRemoteFile.Path = ""
				return nil
			default: // currentRemoteFile.Path > localPath  - we added a new file locally
				changes = append(changes, &Change{ChangeSourceLocal, localPath, ChangeTypeAdded})
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// remaining remote files
	if currentRemoteFile.Path != "" {
		changes = append(changes, &Change{ChangeSourceLocal, currentRemoteFile.Path, ChangeTypeRemoved})
	}
	for currentRemoteFile = range left {
		changes = append(changes, &Change{ChangeSourceLocal, currentRemoteFile.Path, ChangeTypeRemoved})
	}
	return changes, nil
}

// ListRemote - Lists objects from a remote uri and inserts them into the objects channel
func ListRemote(ctx context.Context, client api.ClientWithResponsesInterface, loc *uri.URI, objects chan<- api.ObjectStats) error {
	hasMore := true
	var after string
	defer func() {
		close(objects)
	}()

	for hasMore {
		listResp, err := client.ListObjectsWithResponse(ctx, loc.Repository, loc.Ref, &api.ListObjectsParams{
			After:        (*api.PaginationAfter)(swag.String(after)),
			Prefix:       (*api.PaginationPrefix)(loc.Path),
			UserMetadata: swag.Bool(true),
		})
		if err != nil {
			return err
		}

		if listResp.HTTPResponse.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP %d: %w", listResp.StatusCode(), ErrRemoteDiffFailed)
		}
		for _, o := range listResp.JSON200.Results {
			path := strings.TrimPrefix(o.Path, loc.GetPath())
			// skip directory markers
			if path == "" || (strings.HasSuffix(path, uri.PathSeparator) && swag.Int64Value(o.SizeBytes) == 0) {
				continue
			}
			path = strings.TrimPrefix(path, uri.PathSeparator)
			objects <- api.ObjectStats{
				Checksum:        o.Checksum,
				ContentType:     o.ContentType,
				Metadata:        o.Metadata,
				Mtime:           o.Mtime,
				Path:            path,
				PathType:        o.PathType,
				PhysicalAddress: o.PhysicalAddress,
				SizeBytes:       o.SizeBytes,
			}
		}
		hasMore = listResp.JSON200.Pagination.HasMore
		after = listResp.JSON200.Pagination.NextOffset
	}
	return nil
}

func diffShouldIgnore(name string) bool {
	switch name {
	case IndexFileName, ".DS_Store":
		return true
	default:
		return false
	}
}
