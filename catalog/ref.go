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

	InternalObjectRefSeparator = "$"
	InternalObjectRefFormat    = "int:pbm:%s"
	InternalObjectRefParts     = 3
)

type Ref struct {
	Branch   string
	CommitID CommitID
}

func (r Ref) String() string {
	switch r.CommitID {
	case CommittedID:
		return r.Branch + CommittedSuffix
	case UncommittedID:
		return r.Branch
	default:
		ref := r.Branch + ":" + strconv.Itoa(int(r.CommitID))
		encRef := base58.Encode([]byte(ref))
		return CommitPrefix + encRef
	}
}

func MakeReference(branch string, commitID CommitID) string {
	return Ref{Branch: branch, CommitID: commitID}.String()
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
	const refPartsCount = 2
	parts := strings.SplitN(string(refData), ":", refPartsCount)
	if len(parts) != refPartsCount {
		return nil, fmt.Errorf("%w: missing commit id", ErrInvalidReference)
	}
	id, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: invalid commit id", ErrInvalidReference)
	}
	return &Ref{
		Branch:   parts[0],
		CommitID: CommitID(id),
	}, nil
}

// InternalObjectRef provides information that uniquely identifies an object between
// transactions.  It might be invalidated by some database changes.
type InternalObjectRef struct {
	BranchID  int64
	MinCommit CommitID
	Path      string
}

func (sor *InternalObjectRef) String() string {
	internalRef := fmt.Sprintf("%x%s%x%s%s", sor.BranchID, InternalObjectRefSeparator, sor.MinCommit, InternalObjectRefSeparator, sor.Path)
	return fmt.Sprintf(InternalObjectRefFormat, base58.Encode([]byte(internalRef)))
}

func ParseInternalObjectRef(refString string) (InternalObjectRef, error) {
	var encodedInternalRef string
	_, err := fmt.Sscanf(refString, InternalObjectRefFormat, &encodedInternalRef)
	if err != nil {
		return InternalObjectRef{}, fmt.Errorf("unpack internal object format prefix: %w", err)
	}
	internalRefBytes, err := base58.Decode(encodedInternalRef)
	if err != nil {
		return InternalObjectRef{}, fmt.Errorf("decode internal object bytes %s: %w", encodedInternalRef, err)
	}
	internalRef := string(internalRefBytes)
	parts := strings.SplitN(internalRef, InternalObjectRefSeparator, InternalObjectRefParts)
	if len(parts) < InternalObjectRefParts {
		return InternalObjectRef{}, fmt.Errorf("%w: expected %d parts in internal object content, found %d",
			ErrInvalidReference, InternalObjectRefParts, len(parts))
	}
	branchID, err := strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return InternalObjectRef{}, fmt.Errorf("bad branchID part 0 in %s: %w", internalRef, err)
	}
	minCommit, err := strconv.ParseInt(parts[1], 16, 64)
	if err != nil {
		return InternalObjectRef{}, fmt.Errorf("bad minCommit part 1 in %s: %w", internalRef, err)
	}
	path := parts[2]
	return InternalObjectRef{BranchID: branchID, MinCommit: CommitID(minCommit), Path: path}, nil
}
