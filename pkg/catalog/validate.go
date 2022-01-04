package catalog

import (
	"fmt"

	"github.com/treeverse/lakefs/pkg/validator"
)

const (
	MaxPathLength = 1024
)

func validatePath(v interface{}) error {
	s, ok := v.(Path)
	if !ok {
		panic(validator.ErrInvalidType)
	}

	l := len(s)
	if l == 0 {
		return validator.ErrPathRequiredValue
	}
	if l > MaxPathLength {
		return fmt.Errorf("%w: %d is above maximum length (%d)", validator.ErrInvalidValue, l, MaxPathLength)
	}
	return nil
}

var validatePathOptional = validator.MakeValidateOptional(validatePath)
