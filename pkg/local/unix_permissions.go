package local

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	// DefaultFilePermissions Octal representation of default file permissions
	DefaultFilePermissions     = 0o100666
	UnixPermissionsMetadataKey = apiutil.LakeFSMetadataPrefix + "unix-permissions"
)

var (
	// umask - internal, init only once. Use only via getDefaultPermissions call
	umask = -1
	// defaultOwnership - internal, init only once. Use only via getDefaultPermissions call
	defaultOwnership  *unixOwnership
	getOwnershipMutex sync.Mutex
)

type unixOwnership struct {
	UID int
	GID int
}

type UnixPermissions struct {
	unixOwnership
	Mode os.FileMode
}

func getUmask() int {
	if umask < 0 {
		umask = syscall.Umask(0)
		syscall.Umask(umask)
	}
	return umask
}

func getUnixOwnership(info os.FileInfo) unixOwnership {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return unixOwnership{
			UID: int(stat.Uid),
			GID: int(stat.Gid),
		}
	} else {
		// we are not in linux, this won't work anyway in windows, but maybe you want to log warnings
		return unixOwnership{
			UID: os.Getuid(),
			GID: os.Getgid(),
		}
	}
}

func getDefaultPermissions(isDir bool) UnixPermissions {
	getOwnershipMutex.Lock()
	defer getOwnershipMutex.Unlock()
	mode := DefaultFilePermissions - getUmask()
	if isDir {
		mode = DefaultDirectoryPermissions - getUmask()
	}
	if defaultOwnership == nil {
		defaultOwnership = &unixOwnership{
			UID: os.Getuid(),
			GID: os.Getgid(),
		}
	}
	return UnixPermissions{
		unixOwnership: *defaultOwnership,
		Mode:          os.FileMode(mode),
	}
}

// getUnixPermissionFromStats - Get unix mode and ownership from object metadata, fallback to default permissions in case metadata doesn't exist
func getUnixPermissionFromStats(stats apigen.ObjectStats) (*UnixPermissions, error) {
	permissions := getDefaultPermissions(strings.HasSuffix(stats.Path, uri.PathSeparator))
	if stats.Metadata != nil {
		unixPermissions, ok := stats.Metadata.Get(UnixPermissionsMetadataKey)
		if ok {
			// Unmarshal struct
			if err := json.Unmarshal([]byte(unixPermissions), &permissions); err != nil {
				return nil, err
			}
		}
	}

	return &permissions, nil
}

func isPermissionsChanged(local os.FileInfo, remoteFileStats apigen.ObjectStats) bool {
	localOwnership := getUnixOwnership(local)

	remote, err := getUnixPermissionFromStats(remoteFileStats)
	if err != nil {
		return true
	}

	return local.Mode().Perm() != remote.Mode.Perm() ||
		localOwnership.UID != remote.unixOwnership.UID ||
		localOwnership.GID != remote.unixOwnership.GID
}
