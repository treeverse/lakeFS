package rocks

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/treeverse/lakefs/graveler"
)

var (
	validRefRegexp            = regexp.MustCompile(`^[^\s]+$`)
	validBranchNameRegexp     = regexp.MustCompile(`^\w[-\w]*$`)
	validRepositoryNameRegexp = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
)

var ErrInvalidValue = errors.New("invalid value")

type ValidateFunc func(v interface{}) bool

func Validate(name string, value interface{}, fn ValidateFunc) error {
	if fn(value) {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrInvalidValue, name)
}

func IsNonEmptyString(v interface{}) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	return len(s) > 0
}

func IsValidRef(v interface{}) bool {
	s, ok := v.(graveler.Ref)
	if !ok {
		return false
	}
	return validRefRegexp.MatchString(s.String())
}

func IsValidBranchID(v interface{}) bool {
	s, ok := v.(graveler.BranchID)
	if !ok {
		return false
	}
	return validBranchNameRegexp.MatchString(s.String())
}

func IsValidTagID(v interface{}) bool {
	s, ok := v.(graveler.TagID)
	if !ok {
		return false
	}
	return validBranchNameRegexp.MatchString(string(s))
}

func IsValidCommitID(v interface{}) bool {
	s, ok := v.(graveler.CommitID)
	if !ok {
		return false
	}
	// TODO(barak): identify what is a valid commit ID
	return len(s.String()) > 0
}

func IsValidRepositoryID(v interface{}) bool {
	s, ok := v.(graveler.RepositoryID)
	if !ok {
		return false
	}
	return validRepositoryNameRegexp.MatchString(s.String())
}
