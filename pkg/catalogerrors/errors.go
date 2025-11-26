package catalogerrors

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
)

var (
	// ErrPathRequiredValue indicates missing path
	ErrPathRequiredValue = fmt.Errorf("missing path: %w", graveler.ErrRequiredValue)

	// ErrNotImplemented indicates functionality not implemented
	ErrNotImplemented = errors.New("functionality not implemented")

	// ErrInvalidImportSource indicates invalid import source
	ErrInvalidImportSource = errors.New("invalid import source")
)
