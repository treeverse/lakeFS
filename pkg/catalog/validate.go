package catalog

import (
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/validator"
)

const (
	MaxPathLength = 1024
)

func ValidatePath(v interface{}) error {
	s, ok := v.(Path)
	if !ok {
		panic(graveler.ErrInvalidType)
	}

	l := len(s)
	if l == 0 {
		return ErrPathRequiredValue
	}
	if l > MaxPathLength {
		return fmt.Errorf("%w: %d is above maximum length (%d)", graveler.ErrInvalidValue, l, MaxPathLength)
	}
	return nil
}

var ValidatePathOptional = validator.MakeValidateOptional(ValidatePath)
