package catalog

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/mr-tron/base58"
)

type CommitID int64

const (
	CommittedID   CommitID = -1
	UncommittedID CommitID = 0
	MaxCommitID   CommitID = 1_000_000_000_000_000_000

	CommittedSuffix = ":HEAD"
	CommitPrefix    = "~"
)

type Ref struct {
	Branch   string
	CommitID CommitID
}

func (r Ref) String() string {
	if r.Branch == "" {
		return ""
	}
	switch r.CommitID {
	case CommittedID:
		return r.Branch + CommittedSuffix
	case UncommittedID:
		return r.Branch
	default:
		ref := strconv.Itoa(int(r.CommitID))
		encRef := base58.Encode([]byte(ref))
		return CommitPrefix + encRef
	}
}

func MakeReference(branch string, commitID CommitID) string {
	return Ref{Branch: branch, CommitID: commitID}.String()
}

func MakeCommitReference(commitID CommitID) string {
	return Ref{CommitID: commitID}.String()
}

func ParseRef(ref string) (*Ref, error) {
	// committed branch
	if strings.HasSuffix(ref, CommittedSuffix) {
		return &Ref{
			Branch:   strings.TrimRight(ref, CommittedSuffix),
			CommitID: CommittedID,
		}, nil
	}
	// uncommitted branch
	if !strings.HasPrefix(ref, CommitPrefix) {
		return &Ref{
			Branch:   ref,
			CommitID: UncommittedID,
		}, nil
	}
	// specific commit
	refData, err := base58.Decode(ref[1:])
	if err != nil {
		return nil, fmt.Errorf("%w: ref decode", ErrInvalidReference)
	}
	if !utf8.Valid(refData) {
		return nil, fmt.Errorf("%w: ref utf8", ErrInvalidReference)
	}
	id, err := strconv.Atoi(string(refData))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid commit id", ErrInvalidReference)
	}
	return &Ref{
		CommitID: CommitID(id),
	}, nil
}
