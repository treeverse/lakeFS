package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
)

var (
	ErrPathRequiredValue        = fmt.Errorf("missing path: %w", graveler.ErrRequiredValue)
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata src format")
	ErrExpired                  = errors.New("expired from storage")
)
