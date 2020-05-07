package index

import (
	"fmt"
	"regexp"

	indexerrors "github.com/treeverse/lakefs/index/errors"
)

var (
	ErrInvalidBranchName    = fmt.Errorf("branch name: %w", indexerrors.ErrInvalid)
	ErrInvalidRef           = fmt.Errorf("ref: %w", indexerrors.ErrInvalid)
	ErrInvalidRepositoryId  = fmt.Errorf("repository name: %w", indexerrors.ErrInvalid)
	ErrInvalidPath          = fmt.Errorf("path: %w", indexerrors.ErrInvalid)
	ErrInvalidCommitId      = fmt.Errorf("commit ID: %w", indexerrors.ErrInvalid)
	ErrInvalidCommitMessage = fmt.Errorf("commit message: %w", indexerrors.ErrInvalid)
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
func ValidateOrEmpty(vFunc ValidationFunc, str string) error {
	if len(str) == 0 {
		return nil
	}
	return vFunc(str)
}
func ValidateAll(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
