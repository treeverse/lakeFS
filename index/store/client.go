package store

import (
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/model"

	"github.com/golang/protobuf/proto"
)

type ClientReadOnlyOperations interface {
	ListRepos(amount int, after string) ([]*model.Repo, bool, error)
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

func (c *KVClientReadOnlyOperations) ListRepos(amount int, after string) ([]*model.Repo, bool, error) {
	repos := make([]*model.Repo, 0)

	var iter db.Iterator
	var iterClose db.IteratorCloseFn
	if len(after) > 0 {
		iter, iterClose = c.query.RangePrefixGreaterThan(SubspaceRepos, db.CompositeKey{}, db.CompositeStrings(after))
	} else {
		iter, iterClose = c.query.Range(SubspaceRepos)
	}
	defer iterClose()

	var curr int
	var done, hasMore bool
	for iter.Advance() {
		if done {
			hasMore = true
			break
		}
		kv, err := iter.Get()
		if err != nil {
			return nil, false, err
		}
		repo := &model.Repo{}
		err = proto.Unmarshal(kv.Value, repo)
		if err != nil {
			return nil, false, err
		}
		repos = append(repos, repo)
		curr++
		if curr == amount {
			done = true
		}
	}
	return repos, hasMore, nil
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
	}

	for _, ns := range namespaces {
		err := c.query.ClearChildren(ns, db.CompositeStrings(repoId))
		if err != nil {
			return err
		}
	}

	err := c.query.Delete(SubspaceRepos, db.CompositeStrings(repoId))
	if err != nil {
		return err
	}
	return nil
}

func (s *KVClientOperations) WriteRepo(repo *model.Repo) error {
	return s.query.SetProto(repo, SubspaceRepos, db.CompositeStrings(repo.GetRepoId()))
}
