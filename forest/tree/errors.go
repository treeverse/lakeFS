package tree

import (
	"errors"
)

var (
	ErrPathBiggerThanMaxPath = errors.New(" requested path bigger than biggest path in tree")
	ErrScannerIsNil          = errors.New(" part scanner is nil")
	ErrCloseCrashed          = errors.New(" close GoRoutine crashed")
	ErrEmptyInputToApply     = errors.New(" apply got no input")
	ErrTreeCorrupted         = errors.New(" highest path in tree not in ssTable")
	InfoBaseTreeExhausted    = errors.New(" no tree parts remain")
	ErrDuplicateKey          = errors.New(" Can not write same key twice to SST")
	ErrPushBackTwice         = errors.New(" can't push back twice with no intervening Next")
	ErrEmptyTree             = errors.New(" Empty tree")
)
