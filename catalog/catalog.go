package catalog

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/index"

	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/index/model"
)

type Catalog interface {
	WithContext(ctx context.Context) index.Index
	//?Tree(repoId, branch string) error
	//ReadObject(repoId, ref, path string, readUncommitted bool) (*model.Object, error)
	ReadObject(branchId int, path string, readUncommitted bool) (*model.Object, error)
	//- not needed - ReadEntryObject(repoId, ref, path string, readUncommitted bool) (*model.Entry, error)
	//? ReadEntryTree(repoId, ref, path string, readUncommitted bool) (*model.Entry, error)
	// - not neede ??? ReadRootObject(repoId, ref string, readUncommitted bool) (*model.Root, error)
	// - not neede ??? WriteObject(repoId, branch, path string, object *model.Object) error
	//WriteEntry(repoId, branch, path string, entry *model.Entry) error
	WriteEntry(branchId int, path string, isVersioned bool, entry *model.Entry) error
	// - not needed ??? WriteFile(repoId, branch, path string, entry *model.Entry, obj *model.Object) error
	// not needed ?? DeleteObject(repoId, branch, path string) error
	// ListObjectsByPrefix(repoId, ref, path, after string, results int, descend, readUncommitted bool) ([]*model.Entry, bool, error)
	ListObjectsByPrefix(branchId int, path, after string, results int, descend, readUncommitted bool) ([]*model.Entry, bool, error)
	ListBranchesByPrefix(repoId string, prefix string, amount int, after string) ([]*model.Branch, bool, error)
	// ResetBranch(repoId, branch string) error
	ResetBranch(branchId int) error
	//CreateBranch(repoId, branch, ref string) (*model.Branch, error)
	CreateBranch(repoId int, branch string, sourceBranchId int) (*model.Branch, error)
	//GetBranch(repoId, branch string) (*model.Branch, error)
	GetBranch(branchId string) (*model.Branch, error)
	//Commit(repoId, branch, message, committer string, metadata map[string]string) (*model.Commit, error)
	Commit(branchId int, message, committer string, metadata map[string]string) (*model.Commit, error)
	// ?????? GetCommit(repoId, commitId string) (*model.Commit, error)
	//GetCommitLog(repoId, fromCommitId string, results int, after string) ([]*model.Commit, bool, error)
	GetRepoCommitLog(repoId int, fromCommitId string, results int, after string) ([]*model.Commit, bool, error)
	GetBranchCommitLog(branchId int, fromCommitId string, results int, after string) ([]*model.Commit, bool, error)
	//DeleteBranch(repoId, branch string) error
	DeleteBranch(branchId int) error
	//Diff(repoId, leftRef, rightRef string) (merkle.Differences, error)
	Diff(leftBranchId, rightBranchId int) (merkle.Differences, error)
	// ???? DiffWorkspace(repoId, branch string) (merkle.Differences, error)

	//RevertCommit(repoId, branch, commit string) error
	RevertCommit(branchId int, commit string) error
	//RevertPath(repoId, branch, path string) error
	RevertPath(branchId int, path string) error
	//RevertObject(repoId, branch, path string) error
	RevertObject(branchId int, path string) error
	//Merge(repoId, source, destination, userId string) (merkle.Differences, error)
	Merge(sourceBranchId, destinationBranchId int, userId string) (merkle.Differences, error)
	CreateRepo(repoId, bucketName, defaultBranch string) error // creates the default branch as well
	ListRepos(amount int, after string) ([]*model.Repo, bool, error)
	//GetRepo(repoId string) (*model.Repo, error)
	GetRepo(repoId int) (*model.Repo, error)
	GetRepoByName(repoName string) (*model.Repo, error)
	DeleteRepo(repoId int) error
	//CreateDedupEntryIfNone(repoId string, dedupId string, objName string) (string, error)
	CreateDedupEntryIfNone(repoId int, dedupId string, objName string) (string, error)
	//CreateMultiPartUpload(repoId, id, path, objectName string, creationDate time.Time) error
	CreateMultiPartUpload(repoId int, path, objectName string, creationDate time.Time) error
	//adMultiPartUpload(repoId, uploadId string) (*model.MultipartUpload, error)
	ReadMultiPartUpload(repoId int, uploadId string) (*model.MultipartUpload, error)
	//DeleteMultiPartUpload(repoId, uploadId string) error
	DeleteMultiPartUpload(repoId int, uploadId string) error
}
