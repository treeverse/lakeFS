package git

import (
	"errors"
	"fmt"
)

var (
	ErrGitError       = errors.New("git error")
	ErrNotARepository = fmt.Errorf("not a git repository")
)
