package errors

import (
	"errors"
)

var (
	ErrRemoteNotFound = errors.New("remote not found")
	ErrNotARepository = errors.New("not a git repository")
	ErrNoGit          = errors.New("no git support")
)
