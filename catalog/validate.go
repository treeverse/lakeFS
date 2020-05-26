package catalog

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrInvalidValue         = errors.New("invalid value")
	ErrInvalidBranchName    = fmt.Errorf("branch name: %w", ErrInvalidValue)
	ErrInvalidRepoID        = fmt.Errorf("repository name: %w", ErrInvalidValue)
	ErrInvalidPath          = fmt.Errorf("path: %w", ErrInvalidValue)
	ErrInvalidCommitID      = fmt.Errorf("commit ID: %w", ErrInvalidValue)
	ErrInvalidCommitMessage = fmt.Errorf("commit message: %w", ErrInvalidValue)
	ErrInvalidBucketName    = fmt.Errorf("bucket name: %w", ErrInvalidValue)
	ErrInvalidBranchID      = fmt.Errorf("branch ID: %w", ErrInvalidValue)

	validBranchNameRE = regexp.MustCompile(`^[a-zA-Z0-9\\-]{2,}$`)
	validRepoIDRE     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
	validCommitIDRE   = regexp.MustCompile(`[a-fA-F0-9]{6,40}`)
	validBucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]+[a-z0-9]$`)
	validIPv4RE       = regexp.MustCompile(`/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/`)
)

type ValidateFunc func() error

func Validate(validators ...ValidateFunc) error {
	for _, validator := range validators {
		if err := validator(); err != nil {
			return err
		}
	}
	return nil
}

func ValidateNonEmptyString(s string, err error) ValidateFunc {
	return func() error {
		if len(s) == 0 {
			return err
		}
		return nil
	}
}

func ValidateRegexp(s string, re *regexp.Regexp, err error) ValidateFunc {
	return func() error {
		if !re.MatchString(s) {
			return err
		}
		return nil
	}
}

func ValidateBranchName(name string) ValidateFunc {
	return ValidateRegexp(name, validBranchNameRE, ErrInvalidBranchName)
}

func ValidateRepoName(name string) ValidateFunc {
	return ValidateRegexp(name, validRepoIDRE, ErrInvalidRepoID)
}

func ValidateCommitID(name string) ValidateFunc {
	return ValidateRegexp(name, validCommitIDRE, ErrInvalidCommitID)
}

func ValidatePath(name string) ValidateFunc {
	return ValidateNonEmptyString(name, ErrInvalidPath)
}

func ValidateCommitMessage(msg string) ValidateFunc {
	return ValidateNonEmptyString(msg, ErrInvalidCommitMessage)
}

func ValidateBucketName(bucket string) ValidateFunc {
	return func() error {
		if len(bucket) < 3 || len(bucket) > 63 {
			return ErrInvalidBucketName
		}
		// bucket with successive periods is invalid.
		if strings.Contains(bucket, "..") {
			return ErrInvalidBucketName
		}
		// bucket cannot have ip address style.
		if validIPv4RE.MatchString(bucket) {
			return ErrInvalidBucketName
		}
		// bucket should begin with alphabet/number and end with alphabet/number,
		// with alphabet/number/.- in the middle.
		if !validBucketNameRE.MatchString(bucket) {
			return ErrInvalidBucketName
		}
		return nil
	}
}

func ValidateBranchID(id int) ValidateFunc {
	return func() error {
		if id <= 0 {
			return ErrInvalidBranchID
		}
		return nil
	}
}
