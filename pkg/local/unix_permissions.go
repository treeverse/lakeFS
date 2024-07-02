package local

import (
	"encoding/json"
	"errors"
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
	// umask - internal, init only once. Use only via GetDefaultPermissions call
	umask = -1
	// defaultOwnership - internal, init only once. Use only via GetDefaultPermissions call
	defaultOwnership  *unixOwnership
	getOwnershipMutex sync.Mutex

	ErrUnsupportedFS = errors.New("unsupported filesystem")
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

// GetDefaultPermissions - returns default permissions as defined by file system. Public for testing purposes
func GetDefaultPermissions(isDir bool) UnixPermissions {
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
	permissions := GetDefaultPermissions(strings.HasSuffix(stats.Path, uri.PathSeparator))
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

func getUnixPermissionFromFileInfo(info os.FileInfo) (*UnixPermissions, error) {
	p := UnixPermissions{}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		p.UID = int(stat.Uid)
		p.GID = int(stat.Gid)
		p.Mode = os.FileMode(stat.Mode)
	} else {
		return nil, ErrUnsupportedFS
	}
	return &p, nil
}

func isPermissionsChanged(localFileInfo os.FileInfo, remoteFileStats apigen.ObjectStats) bool {
	local, err := getUnixPermissionFromFileInfo(localFileInfo)
	if err != nil {
		return true
	}

	remote, err := getUnixPermissionFromStats(remoteFileStats)
	if err != nil {
		return true
	}

	return local.Mode != remote.Mode || local.unixOwnership != remote.unixOwnership
}
