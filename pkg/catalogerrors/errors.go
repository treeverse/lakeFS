package catalogerrors

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
)

var (
	ErrPathRequiredValue   = fmt.Errorf("missing path: %w", graveler.ErrRequiredValue)
	ErrNotImplemented      = errors.New("functionality not implemented")
	ErrInvalidImportSource = errors.New("invalid import source")
)
