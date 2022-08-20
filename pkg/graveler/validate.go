package graveler

import (
	"strings"

	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/validator"
)

func ValidateStorageNamespace(v interface{}) error {
	s, ok := v.(StorageNamespace)
	if !ok {
		panic(ErrInvalidType)
	}

	if len(s) == 0 {
		return ErrRequiredValue
	}
	return nil
}

func ValidateRef(v interface{}) error {
	s, ok := v.(Ref)
	if !ok {
		panic(ErrInvalidType)
	}
	if len(s) == 0 {
		return ErrRequiredValue
	}
	if !validator.ReValidRef.MatchString(s.String()) {
		return ErrInvalidRef
	}
	return nil
}

func ValidateBranchID(v interface{}) error {
	s, ok := v.(BranchID)
	if !ok {
		panic(ErrInvalidType)
	}
	if len(s) == 0 {
		return ErrRequiredValue
	}
	if !validator.ReValidBranchID.MatchString(s.String()) {
		return ErrInvalidBranchID
	}
	return nil
}

func ValidateTagID(v interface{}) error {
	s, ok := v.(TagID)
	if !ok {
		panic(ErrInvalidType)
	}

	// https://git-scm.com/docs/git-check-ref-format
	if len(s) == 0 {
		return ErrRequiredValue
	}

	tagID := s.String()
	if tagID == "@" {
		return ErrInvalidTagID
	}
	if strings.HasSuffix(tagID, ".") || strings.HasSuffix(tagID, ".lock") || strings.HasSuffix(tagID, "/") {
		return ErrInvalidTagID
	}
	if strings.Contains(tagID, "..") || strings.Contains(tagID, "//") || strings.Contains(tagID, "@{") {
		return ErrInvalidTagID
	}
	// Unlike git, we do allow '~'.  That supports migration from our previous ref format where commits started with a tilde.
	if strings.ContainsAny(tagID, "^:?*[\\") {
		return ErrInvalidTagID
	}
	for _, r := range tagID {
		if isControlCodeOrSpace(r) {
			return ErrInvalidTagID
		}
	}
	return nil
}

func isControlCodeOrSpace(r rune) bool {
	const space = 0x20
	return r <= space
}

func ValidateCommitID(v interface{}) error {
	s, ok := v.(CommitID)
	if !ok {
		panic(ErrInvalidType)
	}

	if len(s) == 0 {
		return ErrRequiredValue
	}
	if !ident.IsContentAddress(s.String()) {
		return ErrInvalidCommitID
	}
	return nil
}

func ValidateRepositoryID(v interface{}) error {
	var repositoryID string
	switch s := v.(type) {
	case string:
		repositoryID = s
	case RepositoryID:
		repositoryID = s.String()
	default:
		panic(ErrInvalidType)
	}
	if len(repositoryID) == 0 {
		return ErrRequiredValue
	}
	if !validator.ReValidRepositoryID.MatchString(repositoryID) {
		return ErrInvalidRepositoryID
	}
	return nil
}

func ValidateRequiredStrategy(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		panic(ErrInvalidType)
	}

	if s != "dest-wins" && s != "source-wins" && s != "" {
		return ErrInvalidValue
	}
	return nil
}

var ValidateTagIDOptional = validator.MakeValidateOptional(ValidateTagID)
