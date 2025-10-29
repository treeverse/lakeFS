package local

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	// DefaultFilePermissions Octal representation of default file permissions
	DefaultFilePermissions = 0o100666
)

var (
	// umask - internal, init only once. Use only via GetDefaultPermissions call
	umask = -1
	// defaultOwnership - internal, init only once. Use only via GetDefaultPermissions call
	defaultOwnership  *POSIXOwnership
	getOwnershipMutex sync.Mutex

	ErrUnsupportedFS = errors.New("unsupported filesystem")
)

type POSIXOwnership struct {
	UID int
	GID int
}

type POSIXPermissions struct {
	POSIXOwnership
	Mode os.FileMode
}

// GetDefaultPermissions - returns default permissions as defined by file system. Public for testing purposes
func GetDefaultPermissions(isDir bool) POSIXPermissions {
	getOwnershipMutex.Lock()
	defer getOwnershipMutex.Unlock()
	mode := DefaultFilePermissions - getUmask()
	if isDir {
		mode = DefaultDirectoryPermissions - getUmask()
	}
	if defaultOwnership == nil {
		defaultOwnership = &POSIXOwnership{
			UID: os.Getuid(),
			GID: os.Getgid(),
		}
	}
	return POSIXPermissions{
		POSIXOwnership: *defaultOwnership,
		Mode:           os.FileMode(mode), //nolint:gosec
	}
}

// getPermissionFromStats - Get POSIX mode and ownership from object metadata, fallback to default permissions in case metadata doesn't exist
// and withDefault is true
func getPermissionFromStats(stats apigen.ObjectStats, withDefault bool) (*POSIXPermissions, error) {
	permissions := POSIXPermissions{}
	if withDefault {
		permissions = GetDefaultPermissions(strings.HasSuffix(stats.Path, uri.PathSeparator))
	}

	if stats.Metadata != nil {
		posixPermissions, ok := stats.Metadata.Get(apiutil.POSIXPermissionsMetadataKey)
		if ok {
			// Unmarshal struct
			if err := json.Unmarshal([]byte(posixPermissions), &permissions); err != nil {
				return nil, err
			}
		}
	}

	return &permissions, nil
}

func getPermissionFromFileInfo(info os.FileInfo) (*POSIXPermissions, error) {
	return permissionsFromFileInfo(info)
}

func isPermissionsChanged(localFileInfo os.FileInfo, remoteFileStats apigen.ObjectStats, cfg Config) bool {
	if !cfg.IncludePerm {
		return false
	}
	local, err := getPermissionFromFileInfo(localFileInfo)
	if err != nil {
		return true
	}

	remote, err := getPermissionFromStats(remoteFileStats, false)
	if err != nil {
		return true
	}

	if cfg.IncludeUID && local.UID != remote.UID {
		return true
	}
	if cfg.IncludeGID && local.GID != remote.GID {
		return true
	}

	return local.Mode != remote.Mode
}
