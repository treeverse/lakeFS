package catalog

import (
	"errors"
)

var (
	ErrInvalidReference         = errors.New("invalid reference")
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata src format")

	ErrByteSliceTypeAssertion = errors.New("type assertion to []byte failed")
)
