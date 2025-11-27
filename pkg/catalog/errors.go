package catalog

import (
	"errors"

	"github.com/treeverse/lakefs/pkg/catalogerrors"
)

// Define errors we raise from this package - do not convert underlying errors, optionally wrap if needed to consolidate
var (
	ErrUnknownDiffType          = errors.New("unknown graveler difference type")
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata src format")
	ErrExpired                  = errors.New("expired from storage")
	// ErrItClosed is used to determine the reason for the end of the walk
	ErrItClosed           = errors.New("iterator closed")
	ErrNonEmptyRepository = errors.New("non empty repository")

	ErrPathRequiredValue = catalogerrors.ErrPathRequiredValue
	ErrNotImplemented    = catalogerrors.ErrNotImplemented
)
