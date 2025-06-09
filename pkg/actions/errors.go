package actions

import "errors"

type HookClientError struct {
	Err error
}

func (e *HookClientError) Error() string {
	return e.Err.Error()
}

func (e *HookClientError) Unwrap() error {
	return e.Err
}

func NewHookClientError(err error) error {
	if err == nil {
		return nil
	}

	var hce *HookClientError
	if ok := errors.As(err, &hce); ok {
		return err
	}
	return &HookClientError{Err: err}
}
