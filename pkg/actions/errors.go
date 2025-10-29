package actions

import "errors"

var (
	ErrActionFailed          = errors.New("action failed")
	ErrNotFound              = errors.New("not found")
	ErrNilValue              = errors.New("nil value")
	ErrIfExprNotBool         = errors.New("hook 'if' expression should evaluate to a boolean")
	ErrParamConflict         = errors.New("parameters conflict")
	ErrUnknownHookType       = errors.New("unknown hook type")
	ErrInvalidAction         = errors.New("invalid action")
	ErrInvalidEventParameter = errors.New("invalid event parameter")
)
