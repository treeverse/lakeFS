package tree

import (
	"errors"
)

var (
	ErrPathBiggerThanMaxPath = errors.New(" requested path bigger than biggest path in tree")
	ErrScannerIsNil          = errors.New(" part scanner is nil")
	ErrEmptyTree             = errors.New(" Empty tree")
	ErrNotFound              = errors.New("Key not found")
)
