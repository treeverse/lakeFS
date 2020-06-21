package catalog

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrInvalidValue = errors.New("invalid value")

	validBranchNameRegexp = regexp.MustCompile(`^[a-zA-Z0-9\\-]{2,}$`)
	validRepoIDRegexp     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
	validBucketNameRegexp = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]+[a-z0-9]$`)
	validIPv4Regexp       = regexp.MustCompile(`/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/`)
	validHexRegexp        = regexp.MustCompile(`^[a-fA-F0-9]+$`)
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
			return fmt.Errorf("%w: %s", ErrInvalidValue, v.Name)
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
	return validRepoIDRegexp.MatchString(repository)
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

func ValidateDedupID(id string) ValidateFunc {
	return func() bool {
		return IsValidDedupID(id)
	}
}

func IsValidDedupID(id string) bool {
	return validHexRegexp.MatchString(id)
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

func IsValidBucketName(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	// bucket with successive periods is invalid.
	if strings.Contains(bucket, "..") {
		return false
	}
	// bucket cannot have ip address style.
	if validIPv4Regexp.MatchString(bucket) {
		return false
	}
	// bucket should begin with alphabet/number and end with alphabet/number,
	// with alphabet/number/.- in the middle.
	if !validBucketNameRegexp.MatchString(bucket) {
		return false
	}
	return true
}

func ValidateBucketName(bucket string) ValidateFunc {
	return func() bool {
		return IsValidBucketName(bucket)
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
