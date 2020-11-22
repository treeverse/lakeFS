package mvcc

import (
	"fmt"
	"regexp"

	"github.com/treeverse/lakefs/catalog"
)

var (
	validBranchNameRegexp     = regexp.MustCompile(`^\w[-\w]*$`)
	validRepositoryNameRegexp = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
)

type ValidateFunc func() bool

type ValidateField struct {
	Name    string
	IsValid ValidateFunc
}

type ValidateFields []ValidateField

func Validate(validators ValidateFields) error {
	for _, v := range validators {
		if !v.IsValid() {
			return fmt.Errorf("%w: %s", catalog.ErrInvalidValue, v.Name)
		}
	}
	return nil
}

func IsNonEmptyString(s string) bool {
	return len(s) > 0
}

func ValidateBranchName(branch string) ValidateFunc {
	return func() bool {
		return IsValidBranchName(branch)
	}
}

func IsValidBranchName(branch string) bool {
	return validBranchNameRegexp.MatchString(branch)
}

func ValidateRepositoryName(repository string) ValidateFunc {
	return func() bool {
		return IsValidRepositoryName(repository)
	}
}

func IsValidRepositoryName(repository string) bool {
	return validRepositoryNameRegexp.MatchString(repository)
}

func ValidateReference(reference string) ValidateFunc {
	return func() bool {
		return IsValidReference(reference)
	}
}

func IsValidReference(reference string) bool {
	ref, err := ParseRef(reference)
	if err != nil {
		return false
	}
	if !IsValidBranchName(ref.Branch) {
		return false
	}
	if ref.CommitID < CommittedID {
		return false
	}
	return true
}

func ValidateUploadID(uploadID string) ValidateFunc {
	return func() bool {
		return IsNonEmptyString(uploadID)
	}
}

func ValidatePath(name string) ValidateFunc {
	return func() bool {
		return IsNonEmptyString(name)
	}
}

func ValidatePhysicalAddress(addr string) ValidateFunc {
	return func() bool {
		return IsNonEmptyString(addr)
	}
}

func ValidateCommitMessage(message string) ValidateFunc {
	return func() bool {
		return IsNonEmptyString(message)
	}
}

func ValidateCommitter(name string) ValidateFunc {
	return func() bool {
		return IsNonEmptyString(name)
	}
}

func ValidateStorageNamespace(storageNamespace string) ValidateFunc {
	return func() bool {
		return IsNonEmptyString(storageNamespace)
	}
}

func ValidateOptionalString(s string, validator func(string) bool) ValidateFunc {
	return func() bool {
		if len(s) == 0 {
			return true
		}
		return validator(s)
	}
}
