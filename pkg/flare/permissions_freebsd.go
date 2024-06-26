package flare

import "syscall"

func SetBaselinePermissions(mask int) {
	syscall.Umask(mask)
}
