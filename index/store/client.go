package store

import (
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/model"

	"github.com/golang/protobuf/proto"
)

type ClientReadOnlyOperations interface {
	ListRepos() ([]*model.Repo, error)
	ReadRepo(repoId string) (*model.Repo, error)
}

type ClientOperations interface {
	ClientReadOnlyOperations
	DeleteRepo(repoId string) error
	WriteRepo(repo *model.Repo) error
}

type KVClientReadOnlyOperations struct {
	query db.ReadQuery
	store db.Store
}

type KVClientOperations struct {
	*KVClientReadOnlyOperations
	query db.Query
}

func (c *KVClientReadOnlyOperations) ReadRepo(repoId string) (*model.Repo, error) {
	repo := &model.Repo{}
	return repo, c.query.GetAsProto(repo, SubspaceRepos, db.CompositeStrings(repoId))
}

func (c *KVClientReadOnlyOperations) ListRepos() ([]*model.Repo, error) {
	repos := make([]*model.Repo, 0)
	iter, itclose := c.query.Range(SubspaceRepos)
	defer itclose()
	for iter.Advance() {
		kv, err := iter.Get()
		if err != nil {
			return nil, err
		}
		repo := &model.Repo{}
		err = proto.Unmarshal(kv.Value, repo)
		if err != nil {
			return nil, err
		}
		repos = append(repos, repo)
	}
	return repos, nil
}

func (c *KVClientOperations) DeleteRepo(repoId string) error {
	// clear all workspaces, branches, entries, etc.
	namespaces := []db.Namespace{
		SubspaceWorkspace,
		SubspaceBranches,
		SubspaceCommits,
		SubspaceEntries,
		SubspaceObjects,
		SubspaceRefCounts,
		SubspaceRepos,
	}
	for _, ns := range namespaces {
		err := c.query.ClearPrefix(ns, db.CompositeStrings(repoId))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *KVClientOperations) WriteRepo(repo *model.Repo) error {
	return s.query.SetProto(repo, SubspaceRepos, db.CompositeStrings(repo.GetRepoId()))
}
