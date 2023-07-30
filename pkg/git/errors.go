package git

import (
	"errors"
)

var (
	ErrGitError       = errors.New("git error")
	ErrNotARepository = errors.New("not a git repository")
	ErrNoGit          = errors.New("no git support")
)
