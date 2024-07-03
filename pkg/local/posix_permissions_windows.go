package local

import (
	"os"
)

func getUmask() int {
	return 0
}

func permissionsFromFileInfo(info os.FileInfo) (*POSIXPermissions, error) {
	return nil, ErrUnsupportedFS
}
