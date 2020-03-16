package index

import (
	"regexp"

	"github.com/treeverse/lakefs/index/errors"
	"golang.org/x/xerrors"
)

var (
	ErrInvalidBranchName    = xerrors.Errorf("branch name: %w", errors.ErrInvalid)
	ErrInvalidRef           = xerrors.Errorf("ref: %w", errors.ErrInvalid)
	ErrInvalidRepositoryId  = xerrors.Errorf("repository name: %w", errors.ErrInvalid)
	ErrInvalidPath          = xerrors.Errorf("path: %w", errors.ErrInvalid)
	ErrInvalidCommitId      = xerrors.Errorf("commit ID: %w", errors.ErrInvalid)
	ErrInvalidCommitMessage = xerrors.Errorf("commit message: %w", errors.ErrInvalid)
)

type ValidationFunc func(string) error

func ValidateBranchName(name string) error {
	if !regexp.MustCompile(`^[a-zA-Z0-9\\-]{2,}$`).MatchString(name) {
		return ErrInvalidBranchName
	}
	return nil
}

func ValidateRepoId(name string) error {
	if !regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`).MatchString(name) {
		return ErrInvalidRepositoryId
	}
	return nil
}

func ValidateCommitID(name string) error {
	if !regexp.MustCompile(`[a-fA-F0-9]{6,40}`).MatchString(name) {
		return ErrInvalidCommitId
	}
	return nil
}

func ValidateRef(name string) error {
	err := ValidateCommitID(name)
	if err != nil {
		err = ValidateBranchName(name)
	}
	if err != nil {
		return ErrInvalidRef
	}
	return nil
}

func ValidatePath(name string) error {
	if !regexp.MustCompile(`.*`).MatchString(name) {
		return ErrInvalidPath
	}
	return nil
}

func ValidateCommitMessage(msg string) error {
	if len(msg) == 0 {
		return ErrInvalidCommitMessage
	}
	return nil
}

func ValidateAll(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
