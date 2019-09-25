package indexer

import (
	"versio-index/model"
)

type Repo struct {
	ClientID string
	RepoID   string
}

type Branch string

type Index interface {
	Read(Repo, Branch, string) (*model.Blob, error)
	Write(Repo, Branch, string, *model.Blob) error
	Delete(Repo, Branch, string) error
	List(Repo, Branch, string) ([]*model.Entry, error)
	Reset(Repo, Branch) error
	PartialCommit(Repo, Branch) error
	GC(Repo, Branch, string) error
	Commit(Repo, Branch, committer, message string) error
	Merge(Repo, source, destination Branch) error
	DeleteBranch(Repo, Branch) error
	Checkout(Repo, Branch, commit string) error
}
