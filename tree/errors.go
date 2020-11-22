package tree

import (
	"errors"
)

var (
	ErrPathBiggerThanMaxPath = errors.New("Requested path bigger than biggest path in tree")
)
