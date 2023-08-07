package git

import (
	"errors"
)

var (
	ErrNotARepository = errors.New("not a git repository")
	ErrNoGit          = errors.New("no git support")
)
