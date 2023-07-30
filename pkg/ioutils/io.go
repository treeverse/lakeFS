package ioutils

import "os"

// IsDir Returns true if p is a directory, otherwise false
func IsDir(p string) bool {
	stat, err := os.Stat(p)
	if err != nil {
		return false
	}
	return stat.IsDir()
}
