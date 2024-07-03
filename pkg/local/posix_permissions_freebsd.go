package local

import (
	"os"
	"syscall"
)

func getUmask() int {
	if umask < 0 {
		umask = syscall.Umask(0)
		syscall.Umask(umask)
	}
	return umask
}

func permissionsFromFileInfo(info os.FileInfo) (*POSIXPermissions, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, ErrUnsupportedFS
	}

	defaultOwnership = &POSIXOwnership{
		UID: int(stat.Uid),
		GID: int(stat.Gid),
	}

	return &POSIXPermissions{
		Mode:           os.FileMode(stat.Mode),
		POSIXOwnership: *defaultOwnership,
	}, nil
}
