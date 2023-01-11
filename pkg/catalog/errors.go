package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
)

// Define errors we raise from this package - do not convert underlying errors, optionally wrap if needed to consolidate
var (
	ErrPathRequiredValue        = fmt.Errorf("missing path: %w", graveler.ErrRequiredValue)
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata src format")
	ErrExpired                  = errors.New("expired from storage")
	// ErrItClosed is used to determine the reason for the end of the walk
	ErrItClosed = errors.New("iterator closed")

	ErrFeatureNotSupported = errors.New("feature not supported")
)
