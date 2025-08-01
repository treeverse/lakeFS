package local

import (
	"container/heap"
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/fileutil"
	"github.com/treeverse/lakefs/pkg/gateway/path"
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

type MergeStrategy int

const (
	MergeStrategyNone MergeStrategy = iota
	MergeStrategyThis
	MergeStrategyOther
)

// MergeWith combines changes from two diffs, sorting by lexicographic order.
// If the same path appears in both diffs, it's marked as a conflict.
func (c Changes) MergeWith(other Changes, strategy MergeStrategy) Changes {
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
			switch strategy {
			case MergeStrategyNone:
				result = append(result, &Change{
					Source: c[cIdx].Source,
					Path:   c[cIdx].Path,
					Type:   ChangeTypeConflict,
				})
			case MergeStrategyOther:
				result = append(result, other[oIdx])
			case MergeStrategyThis:
				result = append(result, c[cIdx])
			default:
				panic("invalid merge strategy")
			}
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

func switchSource(source ChangeSource) ChangeSource {
	switch source {
	case ChangeSourceRemote:
		return ChangeSourceLocal
	case ChangeSourceLocal:
		return ChangeSourceRemote
	default:
		panic("invalid change source")
	}
}

// Undo Creates a new list of changes that reverses the given changes list.
func Undo(c Changes) Changes {
	reversed := make(Changes, len(c))
	for i, op := range c {
		switch op.Type {
		case ChangeTypeAdded:
			reversed[i] = &Change{
				Source: switchSource(op.Source),
				Path:   op.Path,
				Type:   ChangeTypeRemoved,
			}
		case ChangeTypeModified:
			reversed[i] = &Change{
				Source: switchSource(op.Source),
				Path:   op.Path,
				Type:   ChangeTypeModified,
			}
		case ChangeTypeRemoved:
			reversed[i] = &Change{
				Source: switchSource(op.Source),
				Path:   op.Path,
				Type:   ChangeTypeModified, // mark as modified so it will trigger download
			}
		case ChangeTypeConflict:
		default:
			// Should never reach
			panic(fmt.Sprintf("got unsupported change type %d in undo", op.Type))
		}
	}
	return reversed
}

// WalkS3 - walk like an Egyptian... ¯\_(ツ)¯\_
// This walker function simulates the way object listing is performed by S3. Contrary to how a standard FS walk function behaves, S3
// does not take into consideration the directory hierarchy. Instead, object paths include the entire path relative to the root and as a result
// the directory or "path separator" is also taken into account when providing the listing in a lexicographical order.
func WalkS3(root string, callbackFunc func(p string, info fs.FileInfo, err error) error) error {
	var stringHeap StringHeap
	dirsInfo := make(map[string]os.FileInfo)

	fpWalkErr := filepath.Walk(root, func(p string, info fs.FileInfo, walkErr error) error {
		if walkErr != nil {
			return callbackFunc(p, nil, walkErr)
		}
		if p == root {
			return nil
		}

		if info.IsDir() {
			// Save encountered directories in a min heap and compare them with the first appearance of a file in that level
			dir := p + path.Separator
			dirsInfo[dir] = info        // save dir info for processing it later
			heap.Push(&stringHeap, dir) // add path separator to dir name and sort it later
			return filepath.SkipDir
		}

		for stringHeap.Len() > 0 {
			dir := stringHeap.Peek().(string)
			if p < dir { // file should be processed before dir
				break
			}
			heap.Pop(&stringHeap) // remove from queue

			fileInfo := dirsInfo[dir]
			if fileInfo == nil {
				return fmt.Errorf("fileInfo not found in dirsInfo [%s]: %w", dir, ErrNotFound)
			}

			if err := callbackFunc(dir, fileInfo, nil); err != nil {
				return err
			}

			if err := WalkS3(dir, callbackFunc); err != nil {
				return err
			}
		}

		// Process the file after we finished processing all the dirs that precede it
		return callbackFunc(p, info, nil)
	})
	if fpWalkErr != nil {
		return fpWalkErr
	}

	// Finally, finished walking over FS, handle remaining dirs
	for stringHeap.Len() > 0 {
		dir := heap.Pop(&stringHeap).(string)

		fileInfo := dirsInfo[dir]
		if fileInfo == nil {
			return fmt.Errorf("fileInfo not found in dirsInfo [%s]: %w", dir, ErrNotFound)
		}

		if err := callbackFunc(dir, fileInfo, nil); err != nil {
			return err
		}

		if err := WalkS3(dir, callbackFunc); err != nil {
			return err
		}
	}
	return nil
}

// DiffLocalWithHead Checks changes between a local directory and the head it is pointing to. The diff check assumes the remote
// is an immutable set so any changes found resulted from changes in the local directory
// left is an object channel which contains results from a remote source. rightPath is the local directory to diff with
func DiffLocalWithHead(left <-chan apigen.ObjectStats, rightPath string, cfg Config) (Changes, error) {
	// left should be the base commit
	changes := make([]*Change, 0)

	var (
		currentRemoteFile apigen.ObjectStats
		hasMore           bool
	)
	err := WalkS3(rightPath, func(p string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		includeFile, err := includeLocalFileInDiff(info, cfg)
		if err != nil || !includeFile {
			// if we can't include the file, we skip it, return the error if any
			return err
		}

		localPath := strings.TrimPrefix(p, rightPath)
		localPath = strings.TrimPrefix(localPath, string(filepath.Separator))
		localPath = filepath.ToSlash(localPath) // normalize to use "/" always

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
				if includeRemoteFileInDiff(currentRemoteFile, cfg) {
					changes = append(changes, &Change{ChangeSourceLocal, currentRemoteFile.Path, ChangeTypeRemoved})
				}
				currentRemoteFile.Path = ""
			case currentRemoteFile.Path == localPath:
				changed, err := hasLocalChange(p, info, currentRemoteFile, cfg)
				if err != nil {
					return err
				}
				if changed {
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
func ListRemote(ctx context.Context, client apigen.ClientWithResponsesInterface, loc *uri.URI, objects chan<- apigen.ObjectStats, includeDirs bool) error {
	hasMore := true
	var after string
	defer func() {
		close(objects)
	}()

	for hasMore {
		listResp, err := client.ListObjectsWithResponse(ctx, loc.Repository, loc.Ref, &apigen.ListObjectsParams{
			After:        (*apigen.PaginationAfter)(swag.String(after)),
			Prefix:       (*apigen.PaginationPrefix)(loc.Path),
			UserMetadata: swag.Bool(true),
		})
		if err != nil {
			return err
		}

		if listResp.HTTPResponse.StatusCode != http.StatusOK {
			return fmt.Errorf("list remote failed. HTTP %d: %w", listResp.StatusCode(), ErrRemoteFailure)
		}
		for _, o := range listResp.JSON200.Results {
			p := strings.TrimPrefix(o.Path, loc.GetPath())
			// skip directory markers
			if !includeDirs && (p == "" || (strings.HasSuffix(p, uri.PathSeparator) && swag.Int64Value(o.SizeBytes) == 0)) {
				continue
			}
			p = strings.TrimPrefix(p, uri.PathSeparator)
			objects <- apigen.ObjectStats{
				Checksum:        o.Checksum,
				ContentType:     o.ContentType,
				Metadata:        o.Metadata,
				Mtime:           o.Mtime,
				Path:            p,
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

var ignoreFileList = []string{
	IndexFileName,
	".DS_Store",
}

func includeLocalFileInDiff(info fs.FileInfo, cfg Config) (bool, error) {
	// Handle directories
	if info.IsDir() {
		return cfg.IncludePerm, nil
	}

	// Skip ignored files
	if slices.Contains(ignoreFileList, info.Name()) {
		return false, nil
	}

	// Handle non-regular files (including symlinks)
	if !info.Mode().IsRegular() {
		if cfg.SkipNonRegularFiles {
			return false, nil
		}
		// Check symlink support for non-regular files
		if cfg.SymlinkSupport && info.Mode()&fs.ModeSymlink != 0 {
			return true, nil
		}
		return false, fmt.Errorf("%s: %w", info.Name(), fileutil.ErrNotARegularFile)
	}

	// Regular files are included
	return true, nil
}

func includeRemoteFileInDiff(currentRemoteFile apigen.ObjectStats, cfg Config) bool {
	return cfg.IncludePerm || !strings.HasSuffix(currentRemoteFile.Path, uri.PathSeparator)
}

// hasLocalChange detects whether the local file at `p` differs from the remote
func hasLocalChange(p string, info os.FileInfo, remote apigen.ObjectStats, cfg Config) (bool, error) {
	// 1. symlink target change always wins
	if cfg.SymlinkSupport && info.Mode()&fs.ModeSymlink != 0 {
		return isSymlinkTargetChanged(p, info, remote), nil
	}

	// 2. size changed? (exclude directories))
	if !info.IsDir() {
		localSize := info.Size()
		if localSize != swag.Int64Value(remote.SizeBytes) {
			return true, nil
		}
	}

	// 3. mtime changed?
	remoteMtime, err := getMtimeFromStats(remote)
	if err != nil {
		return false, err
	}
	localMtime := info.ModTime().Unix()
	if localMtime != remoteMtime {
		return true, nil
	}

	// 4. permissions changed?
	if isPermissionsChanged(info, remote, cfg) {
		return true, nil
	}

	return false, nil
}

// isSymlinkTargetChanged checks if the symlink target has changed between local and remote
func isSymlinkTargetChanged(p string, info fs.FileInfo, remoteFileStats apigen.ObjectStats) bool {
	if info.Mode()&fs.ModeSymlink == 0 {
		return false
	}

	// Get local symlink target
	localTarget, err := os.Readlink(p)
	if err != nil {
		// If we can't read the local symlink target, consider it changed
		return true
	}

	// Get remote symlink target from metadata
	if remoteFileStats.Metadata == nil {
		return true
	}

	remoteTarget, ok := remoteFileStats.Metadata.Get(apiutil.SymlinkMetadataKey)
	if !ok {
		return true
	}

	return localTarget != remoteTarget
}
