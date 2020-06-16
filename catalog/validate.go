package catalog

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrInvalidValue = errors.New("invalid value")

	validBranchNameRE = regexp.MustCompile(`^[a-zA-Z0-9\\-]{2,}$`)
	validRepoIDRE     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
	validBucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]+[a-z0-9]$`)
	validIPv4RE       = regexp.MustCompile(`/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/`)
	validHexRE        = regexp.MustCompile(`^[a-fA-F0-9]+$`)
)

type ValidateFunc func() error

type ValidateFields map[string]ValidateFunc

func Validate(validators ValidateFields) error {
	for field, validator := range validators {
		if err := validator(); err != nil {
			return fmt.Errorf("%w: %s", ErrInvalidValue, field)
		}
	}
	return nil
}

func ValidateNonEmptyString(s string) ValidateFunc {
	return func() error {
		if len(s) == 0 {
			return ErrInvalidValue
		}
		return nil
	}
}

func ValidateRegexp(s string, re *regexp.Regexp) ValidateFunc {
	return func() error {
		if !re.MatchString(s) {
			return ErrInvalidValue
		}
		return nil
	}
}

func ValidateBranchName(name string) ValidateFunc {
	return ValidateRegexp(name, validBranchNameRE)
}

func ValidateRepositoryName(name string) ValidateFunc {
	return ValidateRegexp(name, validRepoIDRE)
}

func ValidateUploadID(uploadID string) ValidateFunc {
	return ValidateNonEmptyString(uploadID)
}

func ValidateDedupID(id string) ValidateFunc {
	return ValidateRegexp(id, validHexRE)
}

func ValidatePath(name string) ValidateFunc {
	return ValidateNonEmptyString(name)
}

func ValidatePhysicalAddress(name string) ValidateFunc {
	return ValidateNonEmptyString(name)
}

func ValidateCommitMessage(msg string) ValidateFunc {
	return ValidateNonEmptyString(msg)
}

func ValidateCommitter(name string) ValidateFunc {
	return ValidateNonEmptyString(name)
}

func ValidateBucketName(bucket string) ValidateFunc {
	return func() error {
		if len(bucket) < 3 || len(bucket) > 63 {
			return ErrInvalidValue
		}
		// bucket with successive periods is invalid.
		if strings.Contains(bucket, "..") {
			return ErrInvalidValue
		}
		// bucket cannot have ip address style.
		if validIPv4RE.MatchString(bucket) {
			return ErrInvalidValue
		}
		// bucket should begin with alphabet/number and end with alphabet/number,
		// with alphabet/number/.- in the middle.
		if !validBucketNameRE.MatchString(bucket) {
			return ErrInvalidValue
		}
		return nil
	}
}
