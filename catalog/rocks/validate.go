package rocks

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/ident"
)

const (
	MaxPathLength = 1024
)

var (
	reValidRef          = regexp.MustCompile(`^[^\s]+$`)
	reValidBranchID     = regexp.MustCompile(`^\w[-\w]*$`)
	reValidRepositoryID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
)

var (
	ErrInvalidValue = errors.New("invalid value")
	ErrInvalidType  = errors.New("invalid type")
)

type ValidateFunc func(v interface{}) error

func Validate(name string, value interface{}, fn ValidateFunc) error {
	err := fn(value)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func ValidateNonEmptyString(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return ErrInvalidType
	}
	if len(s) == 0 {
		return ErrInvalidValue
	}
	return nil
}

func ValidateRef(v interface{}) error {
	s, ok := v.(graveler.Ref)
	if !ok {
		return ErrInvalidType
	}
	if !reValidRef.MatchString(s.String()) {
		return ErrInvalidValue
	}
	return nil
}

func ValidateBranchID(v interface{}) error {
	s, ok := v.(graveler.BranchID)
	if !ok {
		return ErrInvalidType
	}
	if !reValidBranchID.MatchString(s.String()) {
		return ErrInvalidValue
	}
	return nil
}

func ValidateTagID(v interface{}) error {
	s, ok := v.(graveler.TagID)
	if !ok {
		return ErrInvalidType
	}
	if !reValidBranchID.MatchString(string(s)) {
		return ErrInvalidValue
	}
	return nil
}

func ValidateCommitID(v interface{}) error {
	s, ok := v.(graveler.CommitID)
	if !ok {
		return ErrInvalidType
	}
	if !ident.IsContentAddress(s.String()) {
		return ErrInvalidValue
	}
	return nil
}

func ValidateRepositoryID(v interface{}) error {
	s, ok := v.(graveler.RepositoryID)
	if !ok {
		return ErrInvalidType
	}
	if !reValidRepositoryID.MatchString(s.String()) {
		return ErrInvalidValue
	}
	return nil
}

func ValidatePath(v interface{}) error {
	s, ok := v.(Path)
	if !ok {
		return ErrInvalidType
	}
	l := len(s.String())
	if l > MaxPathLength {
		return fmt.Errorf("%w: %d is above maximum length (%d)", ErrInvalidValue, l, MaxPathLength)
	}
	return nil
}
