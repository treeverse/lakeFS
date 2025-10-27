package local

import "errors"

var (
	ErrConflict        = errors.New("conflict")
	ErrDownloadingFile = errors.New("error downloading file")
	ErrRemoteFailure   = errors.New("remote failure")
	ErrNotFound        = errors.New("not found")
)
