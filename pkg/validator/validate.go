package validator

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/treeverse/lakefs/pkg/ident"
)

const (
	MaxPathLength = 1024
)

var (
	ReValidRef          = regexp.MustCompile(`^[^\s]+$`)
	ReValidBranchID     = regexp.MustCompile(`^\w[-\w]*$`)
	ReValidRepositoryID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
)

var (
	ErrInvalid           = errors.New("validation error")
	ErrInvalidType       = fmt.Errorf("invalid type: %w", ErrInvalid)
	ErrRequiredValue     = fmt.Errorf("required value: %w", ErrInvalid)
	ErrInvalidValue      = fmt.Errorf("invalid value: %w", ErrInvalid)
	ErrPathRequiredValue = fmt.Errorf("missing path: %w", ErrRequiredValue)
)

type ValidateFunc func(v interface{}) error

type ValidateArg struct {
	Name  string
	Value interface{}
	Fn    ValidateFunc
}

func Validate(args []ValidateArg) error {
	for _, arg := range args {
		err := arg.Fn(arg.Value)
		if err != nil {
			return fmt.Errorf("argument %s: %w", arg.Name, err)
		}
	}
	return nil
}

func MakeValidateOptional(fn ValidateFunc) ValidateFunc {
	return func(v interface{}) error {
		switch s := v.(type) {
		case string:
			if len(s) == 0 {
				return nil
			}
		case fmt.Stringer:
			if len(s.String()) == 0 {
				return nil
			}
		case nil:
			return nil
		}
		return fn(v)
	}
}

func ValidateStorageNamespace(v interface{}) error {
	storageNamespace := reflect.ValueOf(v).String()
	if reflect.TypeOf(v).String() != "graveler.StorageNamespace" {
		panic(ErrInvalidType)
	}
	if len(storageNamespace) == 0 {
		return ErrRequiredValue
	}
	return nil
}

func ValidateRef(v interface{}) error {
	ref := reflect.ValueOf(v).String()
	if reflect.TypeOf(v).String() != "graveler.Ref" {
		panic(ErrInvalidType)
	}
	if len(ref) == 0 {
		return ErrRequiredValue
	}
	if !ReValidRef.MatchString(ref) {
		return ErrInvalidValue
	}
	return nil
}

func ValidateBranchID(v interface{}) error {
	branchId := reflect.ValueOf(v).String()
	if reflect.TypeOf(v).String() != "graveler.BranchID" {
		panic(ErrInvalidType)
	}

	if len(branchId) == 0 {
		return ErrRequiredValue
	}
	if !ReValidBranchID.MatchString(branchId) {
		return ErrInvalidValue
	}
	return nil
}

func ValidateTagID(v interface{}) error {
	tagId := reflect.ValueOf(v).String()
	if reflect.TypeOf(v).String() != "graveler.TagID" {
		panic(ErrInvalidType)
	}

	// https://git-scm.com/docs/git-check-ref-format
	if len(tagId) == 0 {
		return ErrRequiredValue
	}
	if tagId == "@" {
		return ErrInvalidValue
	}
	if strings.HasSuffix(tagId, ".") || strings.HasSuffix(tagId, ".lock") || strings.HasSuffix(tagId, "/") {
		return ErrInvalidValue
	}
	if strings.Contains(tagId, "..") || strings.Contains(tagId, "//") || strings.Contains(tagId, "@{") {
		return ErrInvalidValue
	}
	// Unlike git, we do allow '~'.  That supports migration from our previous ref format where commits started with a tilde.
	if strings.ContainsAny(tagId, "^:?*[\\") {
		return ErrInvalidValue
	}
	for _, r := range tagId {
		if isControlCodeOrSpace(r) {
			return ErrInvalidValue
		}
	}
	return nil
}

func isControlCodeOrSpace(r rune) bool {
	const space = 0x20
	return r <= space
}

func ValidateCommitID(v interface{}) error {
	commitId := reflect.ValueOf(v).String()
	if reflect.TypeOf(v).String() != "graveler.CommitID" {
		panic(ErrInvalidType)
	}
	if len(commitId) == 0 {
		return ErrRequiredValue
	}
	if !ident.IsContentAddress(commitId) {
		return ErrInvalidValue
	}
	return nil
}

func ValidateRepositoryID(v interface{}) error {
	repositoryId := reflect.ValueOf(v).String()
	if reflect.TypeOf(v).String() != "graveler.RepositoryID" {
		panic(ErrInvalidType)
	}
	if len(repositoryId) == 0 {
		return ErrRequiredValue
	}
	if !ReValidRepositoryID.MatchString(repositoryId) {
		return ErrInvalidValue
	}
	return nil
}

func ValidateRequiredString(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		panic(ErrInvalidType)
	}
	if len(s) == 0 {
		return ErrRequiredValue
	}
	return nil
}

func ValidateNonNegativeInt(v interface{}) error {
	i, ok := v.(int)
	if !ok {
		panic(ErrInvalidType)
	}
	if i < 0 {
		return ErrInvalidValue
	}
	return nil
}

func ValidatePath(v interface{}) error {
	path := reflect.ValueOf(v).String()
	if reflect.TypeOf(v).String() != "catalog.Path" {
		panic(ErrInvalidType)
	}

	l := len(path)
	if l == 0 {
		return ErrPathRequiredValue
	}
	if l > MaxPathLength {
		return fmt.Errorf("%w: %d is above maximum length (%d)", ErrInvalidValue, l, MaxPathLength)
	}
	return nil
}

var ValidateTagIDOptional = MakeValidateOptional(ValidateTagID)
var ValidatePathOptional = MakeValidateOptional(ValidatePath)
