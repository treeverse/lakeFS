package index

import "time"

type EntryType int

const (
	EntryTypeBlob EntryType = iota
	EntryTypeTree
)

type Repo struct {
	ClientID           string
	RepoID             string
	DefaultBranch      string
	PartialCommitRatio float64
}

type Blob struct {
	Address  string
	Blocks   []string
	Metadata map[string]string
}

type Tree struct {
	Address  string
	Metadata map[string]string
}

type Entry struct {
	Type     EntryType
	Address  string
	Name     string
	Metadata map[string]string
}

type Commit struct {
	Address  string
	Metadata map[string]string
	Author   string
	Date     time.Time
	Parents  []*Commit
}

type Branch struct {
	Name          string
	CommitAddress string
	WorkspaceRoot string
}

type Index interface {
	// Workspace
	Read(repo, branch string, path string) (*Blob, error)
	Write(repo, branch string, blob *Blob) error
	Delete(repo, branch string) error
	List(repo, branch string) ([]*Entry, error)

	// index
	Reset(repo, branch string) error
	GC(repo, branch string) error
	Commit(repo, branch, committer, message string) error
	Merge(repo, sourceBranch, destinationBranch string) error
	DeleteBranch(repo, branch string) error
	Checkout(repo, branch, commit string) error
}
