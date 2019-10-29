package store

import (
	"versio-index/db"

	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"

	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	SubspaceRepos     = "repos"
	SubspaceWorkspace = "workspace"
	SubspaceEntries   = "entries"
	SubspaceObjects   = "objects"
	SubspaceCommits   = "commits"
	SubspaceBranches  = "branches"
	SubspaceRefCounts = "refCounts"
)

type Store interface {
	ClientReadTransact(clientId string, fn func(ops ClientReadOnlyOperations) (interface{}, error)) (interface{}, error)
	ClientTransact(clientId string, fn func(ops ClientOperations) (interface{}, error)) (interface{}, error)
	RepoReadTransact(clientId, repoId string, fn func(ops RepoReadOnlyOperations) (interface{}, error)) (interface{}, error)
	RepoTransact(clientId, repoId string, fn func(ops RepoOperations) (interface{}, error)) (interface{}, error)
}

type KVStore struct {
	kv db.Store
}

func NewKVStore(database fdb.Database, dir directory.DirectorySubspace) *KVStore {
	kv := db.NewFDBStore(database, map[string]subspace.Subspace{
		SubspaceRepos:     dir.Sub(SubspaceRepos),
		SubspaceWorkspace: dir.Sub(SubspaceWorkspace),
		SubspaceEntries:   dir.Sub(SubspaceEntries),
		SubspaceObjects:   dir.Sub(SubspaceObjects),
		SubspaceCommits:   dir.Sub(SubspaceCommits),
		SubspaceBranches:  dir.Sub(SubspaceBranches),
		SubspaceRefCounts: dir.Sub(SubspaceRefCounts),
	})
	return &KVStore{kv: kv}
}

func (s *KVStore) ClientReadTransact(clientId string, fn func(ops ClientReadOnlyOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.ReadTransact(tuple.Tuple{clientId}, func(q db.ReadQuery) (interface{}, error) {
		return fn(&KVClientReadOnlyOperations{
			query: q,
			store: s.kv,
		})
	})
}

func (s *KVStore) ClientTransact(clientId string, fn func(ops ClientOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.Transact(tuple.Tuple{clientId}, func(q db.Query) (interface{}, error) {
		return fn(&KVClientOperations{
			KVClientReadOnlyOperations: &KVClientReadOnlyOperations{
				query: q,
				store: s.kv,
			},
			query: q,
		})
	})
}

func (s *KVStore) RepoReadTransact(clientId, repoId string, fn func(ops RepoReadOnlyOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.ReadTransact(tuple.Tuple{clientId, repoId}, func(q db.ReadQuery) (interface{}, error) {
		return fn(&KVRepoReadOnlyOperations{
			query: q,
			store: s.kv,
		})
	})
}

func (s *KVStore) RepoTransact(clientId, repoId string, fn func(ops RepoOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.Transact(tuple.Tuple{clientId, repoId}, func(q db.Query) (interface{}, error) {
		return fn(&KVRepoOperations{
			KVRepoReadOnlyOperations: &KVRepoReadOnlyOperations{
				query: q,
				store: s.kv,
			},
			query: q,
		})
	})
}
