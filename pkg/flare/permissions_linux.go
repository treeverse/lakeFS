package flare

import "syscall"

func setBaselinePermissions(mask int) {
	syscall.Umask(mask)
}
