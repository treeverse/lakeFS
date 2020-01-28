package store

import (
	"github.com/treeverse/lakefs/db"

	"github.com/dgraph-io/badger"
)

var (
	SubspaceRepos                 = db.Namespace("repos")
	SubspaceWorkspace             = db.Namespace("workspace")
	SubspaceEntries               = db.Namespace("entries")
	SubspaceObjects               = db.Namespace("objects")
	SubspaceCommits               = db.Namespace("commits")
	SubspaceBranches              = db.Namespace("branches")
	SubspaceRefCounts             = db.Namespace("refCounts")
	SubspacesMultipartUploads     = db.Namespace("mpus")
	SubspacesMultipartUploadParts = db.Namespace("mpuparts")
)

type Store interface {
	ReadTransact(fn func(ops ClientReadOnlyOperations) (interface{}, error)) (interface{}, error)
	Transact(fn func(ops ClientOperations) (interface{}, error)) (interface{}, error)
	RepoReadTransact(repoId string, fn func(ops RepoReadOnlyOperations) (interface{}, error)) (interface{}, error)
	RepoTransact(repoId string, fn func(ops RepoOperations) (interface{}, error)) (interface{}, error)
}

type KVStore struct {
	kv db.Store
}

func NewKVStore(database *badger.DB) *KVStore {
	kv := db.NewDBStore(database)
	return &KVStore{kv: kv}
}

func (s *KVStore) ReadTransact(fn func(ops ClientReadOnlyOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.ReadTransact(func(q db.ReadQuery) (interface{}, error) {
		return fn(&KVClientReadOnlyOperations{
			query: q,
			store: s.kv,
		})
	})
}

func (s *KVStore) Transact(fn func(ops ClientOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.Transact(func(q db.Query) (interface{}, error) {
		return fn(&KVClientOperations{
			KVClientReadOnlyOperations: &KVClientReadOnlyOperations{
				query: q,
				store: s.kv,
			},
			query: q,
		})
	})
}

func (s *KVStore) RepoReadTransact(repoId string, fn func(ops RepoReadOnlyOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.ReadTransact(func(q db.ReadQuery) (interface{}, error) {
		return fn(&KVRepoReadOnlyOperations{
			query:  q,
			store:  s.kv,
			repoId: repoId,
		})
	})
}

func (s *KVStore) RepoTransact(repoId string, fn func(ops RepoOperations) (interface{}, error)) (interface{}, error) {
	return s.kv.Transact(func(q db.Query) (interface{}, error) {
		return fn(&KVRepoOperations{
			KVRepoReadOnlyOperations: &KVRepoReadOnlyOperations{
				query:  q,
				store:  s.kv,
				repoId: repoId,
			},
			query: q,
		})
	})
}
