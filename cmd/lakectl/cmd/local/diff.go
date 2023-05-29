package local

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
	"io/fs"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	ChangeSourceLocal  = "local"
	ChangeSourceRemote = "remote"
)

var (
	ErrRemoteDiffFailed = errors.New("remote diff failed")
)

type Change struct {
	Source string
	Path   string
	Type   string
}

func (c *Change) String() string {
	return fmt.Sprintf("%s\t%s\t%s", c.Source, c.Type, c.Path)
}

type Changes []*Change

func (c Changes) String() string {
	strs := make([]string, len(c))
	for i, cc := range c {
		strs[i] = cc.String()
	}
	return strings.Join(strs, "\n")
}

func (c Changes) MergeWith(other Changes) Changes {
	cIdx := 0
	oIdx := 0
	result := make(Changes, 0)
	for cIdx < len(c) && oIdx < len(other) {
		if c[cIdx].Path > other[oIdx].Path {
			// other is first
			result = append(result, other[oIdx])
			oIdx++
		} else if c[cIdx].Path < other[oIdx].Path {
			result = append(result, c[cIdx])
			cIdx++
		} else {
			// both modified the same path!!
			result = append(result, &Change{
				Source: c[cIdx].Source,
				Path:   c[cIdx].Path,
				Type:   "conflict",
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

func reversedType(typ string) string {
	switch typ {
	case "added":
		return "removed"
	case "removed":
		return "added"
	default:
		return typ
	}
}

type DiffOpt int16

const (
	DiffTypeTwoWay DiffOpt = iota
)

func DiffRemotes(ctx context.Context, client Client, left, right *uri.URI, opts ...DiffOpt) (Changes, error) {
	var mode *string
	for _, o := range opts {
		if o == DiffTypeTwoWay {
			mode = swag.String("two_dot")
		}
	}

	// left should be the base commit!
	changes := make([]*Change, 0)
	hasMore := true
	var after string
	for hasMore {
		diffResp, err := client.DiffRefsWithResponse(ctx, left.Repository, left.Ref, right.Ref, &api.DiffRefsParams{
			After:  (*api.PaginationAfter)(swag.String(after)),
			Prefix: (*api.PaginationPrefix)(left.Path),
			Type:   mode,
		})
		if err != nil {
			return nil, err
		}
		if diffResp.HTTPResponse.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%w: HTTP %d", ErrRemoteDiffFailed, diffResp.StatusCode())
		}

		for _, d := range diffResp.JSON200.Results {
			c := &Change{
				Source: ChangeSourceRemote,
				Path:   strings.TrimPrefix(d.Path, left.GetPath()),
				Type:   d.Type,
			}
			changes = append(changes, c)
		}

		hasMore = diffResp.JSON200.Pagination.HasMore
		after = diffResp.JSON200.Pagination.NextOffset
	}
	return changes, nil
}

func getMtimeFromStats(stats api.ObjectStats) (int64, error) {
	if stats.Metadata == nil {
		return stats.Mtime, nil
	}
	clientMtime, hasClientMtime := stats.Metadata.Get(ClientMtimeMetadataKey)
	if hasClientMtime {
		// parse
		return strconv.ParseInt(clientMtime, 10, 64)
	}
	return stats.Mtime, nil
}

func switchSource(source string) string {
	if source == "remote" {
		return "local"
	}
	return "remote"
}

func Undo(c Changes) Changes {
	reversed := make(Changes, len(c))
	for i, op := range c {

		switch op.Type {
		case "added":
			reversed[i] = &Change{
				Source: switchSource(op.Source),
				Path:   op.Path,
				Type:   "removed",
			}
		case "changed":
			reversed[i] = &Change{
				Source: switchSource(op.Source),
				Path:   op.Path,
				Type:   "changed",
			}
		case "removed":
			reversed[i] = &Change{
				Source: switchSource(op.Source),
				Path:   op.Path,
				Type:   "changed",
			}
		}
	}
	return reversed
}

func diffShouldIgnore(fname string) bool {
	switch fname {
	case IndexFileName, ".DS_Store":
		return true
	default:
		return false
	}
}

func DiffLocal(left []api.ObjectStats, rightPath string) (Changes, error) {
	// left should be the base commit
	changes := make([]*Change, 0)
	leftIdx := 0
	err := filepath.Walk(rightPath, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() || diffShouldIgnore(info.Name()) {
			return nil
		}
		localPath := strings.TrimPrefix(path, rightPath)
		localPath = strings.TrimPrefix(localPath, string(filepath.Separator))
		localPath = filepath.ToSlash(localPath) // normalize to use "/" always

		localBytes := info.Size()
		localMtime := info.ModTime().Unix()

		// anything left on the left side?
		if leftIdx < len(left) {
			for leftIdx < len(left) {
				currentRemoteFile := left[leftIdx]
				if currentRemoteFile.Path < localPath {
					changes = append(changes, &Change{ChangeSourceLocal, currentRemoteFile.Path, "removed"})
					leftIdx++
				} else if currentRemoteFile.Path == localPath {
					// we have the same file. Are they identical?
					remoteMtime, err := getMtimeFromStats(currentRemoteFile)
					if err != nil {
						return err
					}
					if localBytes != swag.Int64Value(currentRemoteFile.SizeBytes) || localMtime != remoteMtime {
						// we made a change!
						changes = append(changes, &Change{ChangeSourceLocal, localPath, "changed"})
					}
					leftIdx++
					break
				} else {
					changes = append(changes, &Change{ChangeSourceLocal, localPath, "added"})
					break // we haven't gotten this far yet?
				}
			}
		} else {
			// nothing left on the left side, we definitely added stuff!
			changes = append(changes, &Change{ChangeSourceLocal, localPath, "added"})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// remaining remote files
	for leftIdx < len(left) {
		currentRemoteFile := left[leftIdx]
		leftIdx++
		changes = append(changes, &Change{ChangeSourceLocal, currentRemoteFile.Path, "removed"})
	}
	return changes, nil
}
