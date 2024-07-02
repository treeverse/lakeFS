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
	DefaultFilePermissions      = 0o100666
	POSIXPermissionsMetadataKey = apiutil.LakeFSMetadataPrefix + "posix-permissions"
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

func getUmask() int {
	if umask < 0 {
		umask = syscall.Umask(0)
		syscall.Umask(umask)
	}
	return umask
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
		Mode:           os.FileMode(mode),
	}
}

// getPermissionFromStats - Get POSIX mode and ownership from object metadata, fallback to default permissions in case metadata doesn't exist
func getPermissionFromStats(stats apigen.ObjectStats) (*POSIXPermissions, error) {
	permissions := GetDefaultPermissions(strings.HasSuffix(stats.Path, uri.PathSeparator))
	if stats.Metadata != nil {
		posixPermissions, ok := stats.Metadata.Get(POSIXPermissionsMetadataKey)
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
	p := POSIXPermissions{}
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
	local, err := getPermissionFromFileInfo(localFileInfo)
	if err != nil {
		return true
	}

	remote, err := getPermissionFromStats(remoteFileStats)
	if err != nil {
		return true
	}

	return local.Mode != remote.Mode || local.POSIXOwnership != remote.POSIXOwnership
}
