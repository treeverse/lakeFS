package catalog

import (
	"context"
	"time"
)

type EntryState int

const (
	EntryStateCommitted = iota
	EntryStateStage
	EntryStateUnstage
)

type EntryReadOptions struct {
	EntryState
	CommitID int
}

type Cataloger interface {
	WithContext(ctx context.Context) Cataloger

	// repository level
	CreateRepo(repoID string, bucket string, branch string) (int, error)
	ListRepos(amount int, after string) ([]*Repo, bool, error)
	GetRepo(repoID int) (*Repo, error)
	GetRepoByName(repoName string) (*Repo, error)
	DeleteRepo(repoID int) error
	GetRepoCommitLog(repoID int, fromCommitID string, results int, after string) ([]*Commit, bool, error)

	// branch level
	CreateBranch(repoID int, branch string, sourceBranchID int) (*Branch, error)
	GetBranch(branchID string) (*Branch, error)
	DeleteBranch(branchID int) error
	GetBranchCommitLog(branchID int, fromCommitID string, results int, after string) ([]*Commit, bool, error)
	ListBranchesByPrefix(repoID string, prefix string, amount int, after string) ([]*Branch, bool, error)
	Commit(branchID int, message, committer string, metadata map[string]string) (*Commit, error)

	// entry level
	ReadEntry(branchID int, path string, readOptions EntryReadOptions) (*Entry, error)
	WriteEntry(branchID int, path string, entry *Entry) error
	ListEntriesByPrefix(branchID int, path, after string, results int, readOptions EntryReadOptions, descend bool) ([]*Entry, bool, error)

	// diff and merge
	Diff(leftBranchID, rightBranchID int) (Differences, error)
	Merge(sourceBranchID, destinationBranchID int, userID string) (Differences, error)

	// revert
	RevertBranch(branchID int) error
	RevertCommit(branchID int, commit string) error
	RevertPath(branchID int, path string) error
	RevertEntry(branchID int, path string) error

	CreateDedupEntryIfNone(repoID int, dedupID string, physicalAddress string) (string, error)
	CreateMultiPartUpload(repoID int, path, physicalAddress string, creationTime time.Time) error
	ReadMultiPartUpload(repoID int, uploadID string) (*MultipartUpload, error)
	DeleteMultiPartUpload(repoID int, uploadID string) error
}
