package fileutil

import "os"

// IsDir Returns true if p is a directory, otherwise false
func IsDir(p string) (bool, error) {
	stat, err := os.Stat(p)
	if err != nil {
		return false, err
	}
	return stat.IsDir(), nil
}
