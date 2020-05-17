package store

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	indexerrors "github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/index/model"
)

type RepoOperations interface {
	ReadRepo() (*model.Repo, error)
	LockWorkspace() error
	ListWorkspace(branch string) ([]*model.WorkspaceEntry, error)
	ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error)
	ListBranches(prefix string, amount int, after string) ([]*model.Branch, bool, error)
	ReadBranch(branch string) (*model.Branch, error)
	ReadRoot(addr string) (*model.Root, error)
	ReadObject(addr string) (*model.Object, error)
	ReadCommit(addr string) (*model.Commit, error)
	ListTree(addr, after string, results int) ([]*model.Entry, bool, error)
	ListTreeWithPrefix(addr, prefix, after string, results int) ([]*model.Entry, bool, error)
	ReadTreeEntry(treeAddress, name string) (*model.Entry, error)

	// Multipart uploads
	ReadMultipartUpload(uploadId string) (*model.MultipartUpload, error)
	ListMultipartUploads() ([]*model.MultipartUpload, error)
	GetObjectDedup(DedupId string) (*model.ObjectDedup, error)
	DeleteWorkspacePath(branch, path string) error
	WriteToWorkspacePath(branch, parentPath, path string, entry *model.WorkspaceEntry) error
	ClearWorkspace(branch string) error
	WriteTree(address string, entries []*model.Entry) error
	WriteRoot(address string, root *model.Root) error
	WriteObject(addr string, object *model.Object) error
	WriteCommit(addr string, commit *model.Commit) error
	WriteBranch(name string, branch *model.Branch) error
	DeleteBranch(name string) error
	WriteRepo(repo *model.Repo) error
	WriteMultipartUpload(upload *model.MultipartUpload) error
	DeleteMultipartUpload(uploadId string) error
	WriteObjectDedup(dedup *model.ObjectDedup) error
}

type DBRepoOperations struct {
	repoId string
	tx     db.Tx
}

func (o *DBRepoOperations) ReadRepo() (*model.Repo, error) {
	repo := &model.Repo{}
	err := o.tx.Get(repo, `SELECT * FROM repositories WHERE id = $1`, o.repoId)
	return repo, err
}

func (o *DBRepoOperations) LockWorkspace() error {
	_, err := o.tx.Exec(`LOCK TABLE workspace_entries IN ACCESS EXCLUSIVE MODE`)
	return err
}

func (o *DBRepoOperations) ListWorkspace(branch string) ([]*model.WorkspaceEntry, error) {
	var entries []*model.WorkspaceEntry
	err := o.tx.Select(
		&entries,
		`SELECT * FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2`,
		o.repoId, branch)
	return entries, err
}

func (o *DBRepoOperations) ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error) {
	ent := &model.WorkspaceEntry{}
	err := o.tx.Get(ent, `SELECT * FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2 AND path = $3`,
		o.repoId, branch, path)
	return ent, err
}

func (o *DBRepoOperations) ListBranches(prefix string, amount int, after string) ([]*model.Branch, bool, error) {
	var err error
	var hasMore bool
	var branches []*model.Branch

	prefixCond := db.Prefix(prefix)
	if amount >= 0 {
		err = o.tx.Select(&branches, `SELECT * FROM branches WHERE repository_id = $1 AND id like $2 AND id > $3 ORDER BY id ASC LIMIT $4`,
			o.repoId, prefixCond, after, amount+1)
	} else {
		err = o.tx.Select(&branches, `SELECT * FROM branches WHERE repository_id = $1 AND id like $2 AND id > $3 ORDER BY id ASC`,
			o.repoId, prefixCond, after)
	}

	if err != nil {
		return nil, false, err
	}

	if amount >= 0 && len(branches) > amount {
		branches = branches[0:amount]
		hasMore = true
	}

	return branches, hasMore, err
}

func (o *DBRepoOperations) ReadBranch(branch string) (*model.Branch, error) {
	b := &model.Branch{}
	err := o.tx.Get(b, `SELECT * FROM branches WHERE repository_id = $1 AND id = $2`, o.repoId, branch)
	if errors.Is(err, db.ErrNotFound) {
		err = indexerrors.ErrBranchNotFound
	}
	return b, err
}

func (o *DBRepoOperations) ReadRoot(address string) (*model.Root, error) {
	r := &model.Root{}
	err := o.tx.Get(r, `SELECT * FROM roots WHERE repository_id = $1 AND address = $2`, o.repoId, address)
	return r, err
}

func (o *DBRepoOperations) ReadObject(addr string) (*model.Object, error) {
	obj := &model.Object{}
	err := o.tx.Get(obj, `SELECT * FROM objects WHERE repository_id = $1 AND object_address = $2`, o.repoId, addr)
	return obj, err
}

func (o *DBRepoOperations) ReadCommit(addr string) (*model.Commit, error) {
	commit := &model.Commit{}
	var err error
	if len(addr) > ident.HashHexLength || len(addr) < 6 {
		return nil, db.ErrNotFound
	}

	if len(addr) == ident.HashHexLength {
		err = o.tx.Get(commit, `SELECT * FROM commits WHERE repository_id = $1 AND address = $2`, o.repoId, addr)
		return commit, err
	}

	// otherwise, it could be a truncated commit hash - we support this as long as the results are not ambiguous
	// i.e. a truncated commit returns 1 - and only 1 result.
	var commits []*model.Commit
	err = o.tx.Select(&commits, `SELECT * FROM commits WHERE repository_id = $1 AND address LIKE $2 LIMIT 2`,
		o.repoId, fmt.Sprintf("%s%%", addr))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, db.ErrNotFound
		}
		return nil, err
	}
	if len(commits) != 1 {
		return nil, db.ErrNotFound
	}
	return commits[0], err
}

func (o *DBRepoOperations) ListTreeWithPrefix(addr, prefix, after string, amount int) ([]*model.Entry, bool, error) {
	var err error
	var hasMore bool
	var entries []*model.Entry

	if len(after) == 0 {
		if amount >= 0 {
			err = o.tx.Select(&entries, `SELECT * FROM entries WHERE repository_id = $1 AND parent_address = $2 AND name LIKE $3 ORDER BY name ASC LIMIT $4`,
				o.repoId, addr, db.Prefix(prefix), amount+1)
		} else {
			err = o.tx.Select(&entries, `SELECT * FROM entries WHERE repository_id = $1 AND parent_address = $2 AND name LIKE $3 ORDER BY name ASC`,
				o.repoId, addr, db.Prefix(prefix))
		}
	} else {
		if amount >= 0 {
			err = o.tx.Select(&entries, `SELECT * FROM entries WHERE repository_id = $1 AND parent_address = $2 AND name LIKE $3 AND name > $4 ORDER BY name ASC LIMIT $5`,
				o.repoId, addr, db.Prefix(prefix), after, amount+1)
		} else {
			err = o.tx.Select(&entries, `SELECT * FROM entries WHERE repository_id = $1 AND parent_address = $2 AND name LIKE $3 AND name > $4 ORDER BY name ASC`,
				o.repoId, addr, db.Prefix(prefix), after)
		}
	}

	if err != nil {
		return nil, false, err
	}

	if amount >= 0 && len(entries) > amount {
		entries = entries[0:amount]
		hasMore = true
	}

	return entries, hasMore, err
}

func (o *DBRepoOperations) ListTree(addr, after string, results int) ([]*model.Entry, bool, error) {
	return o.ListTreeWithPrefix(addr, "", after, results)
}

func (o *DBRepoOperations) ReadTreeEntry(treeAddress, name string) (*model.Entry, error) {
	entry := &model.Entry{}
	err := o.tx.Get(entry, `SELECT * FROM entries WHERE repository_id = $1 AND parent_address = $2 AND name = $3`,
		o.repoId, treeAddress, name)
	return entry, err
}

func (s *DBRepoOperations) GetObjectDedup(dedupId string) (*model.ObjectDedup, error) {
	m := &model.ObjectDedup{}
	err := s.tx.Get(m, `SELECT repository_id,encode(dedup_id,'hex') as dedup_id,physical_address FROM object_dedup WHERE repository_id = $1 AND dedup_id = decode($2,'hex')`,
		s.repoId, dedupId)
	return m, err
}

func (o *DBRepoOperations) ReadMultipartUpload(uploadId string) (*model.MultipartUpload, error) {
	m := &model.MultipartUpload{}
	err := o.tx.Get(m, `SELECT  repository_id,upload_id,path,creation_date,physical_address FROM multipart_uploads WHERE repository_id = $1 AND upload_id = $2`,
		o.repoId, uploadId)
	return m, err
}

func (o *DBRepoOperations) ListMultipartUploads() ([]*model.MultipartUpload, error) {
	var mpus []*model.MultipartUpload
	err := o.tx.Select(&mpus, `SELECT repository_id,upload_id,path,creation_date,physical_address FROM multipart_uploads WHERE repository_id = $1`, o.repoId)
	return mpus, err
}

func (o *DBRepoOperations) DeleteWorkspacePath(branch, path string) error {
	_, err := o.tx.Exec(`DELETE FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2 AND path = $3`,
		o.repoId, branch, path)
	return err
}

func (o *DBRepoOperations) WriteToWorkspacePath(branch, parentPath, path string, entry *model.WorkspaceEntry) error {
	_, err := o.tx.Exec(`
		INSERT INTO workspace_entries (repository_id, branch_id, path, parent_path, 
		entry_name, entry_address, entry_type, entry_creation_date, entry_size, entry_checksum, tombstone)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT ON CONSTRAINT workspace_entries_pkey
		DO UPDATE SET parent_path = $4, entry_name = $5, entry_address = $6, entry_type = $7, entry_creation_date = $8,
		entry_size = $9, entry_checksum = $10, tombstone = $11`,
		o.repoId, branch, path, parentPath, entry.EntryName, entry.EntryAddress, entry.EntryType,
		entry.EntryCreationDate, entry.EntrySize, entry.EntryChecksum, entry.Tombstone,
	)
	return err
}

func (s *DBRepoOperations) WriteObjectDedup(dedup *model.ObjectDedup) error {

	_, err := s.tx.Exec(`INSERT INTO object_dedup  (repository_id, dedup_id,physical_address) values ($1,decode($2,'hex'),$3)`,
		s.repoId, dedup.DedupId, dedup.PhysicalAddress)
	return err
}

func (o *DBRepoOperations) ClearWorkspace(branch string) error {
	_, err := o.tx.Exec(`DELETE FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2`,
		o.repoId, branch)
	return err
}

func (o *DBRepoOperations) WriteTree(address string, entries []*model.Entry) error {
	for _, entry := range entries {
		_, err := o.tx.Exec(`
			INSERT INTO entries (repository_id, parent_address, name, address, type, creation_date, size, checksum)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT ON CONSTRAINT entries_pkey
			DO NOTHING`,
			o.repoId, address, entry.Name, entry.Address, entry.EntryType, entry.CreationDate, entry.Size, entry.Checksum)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *DBRepoOperations) WriteRoot(address string, root *model.Root) error {
	_, err := o.tx.Exec(`
		INSERT INTO roots (repository_id, address, creation_date, size) VALUES ($1, $2, $3, $4)
		ON CONFLICT ON CONSTRAINT roots_pkey
		DO NOTHING`,
		o.repoId, address, root.CreationDate, root.Size)
	return err
}

func (o *DBRepoOperations) WriteObject(addr string, object *model.Object) error {
	object.ObjectAddress = addr
	_, err := o.tx.Exec(`
		INSERT INTO objects (repository_id, object_address, checksum, size, physical_address, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT ON CONSTRAINT objects_pkey
		DO NOTHING`, // since it's keyed by hash of content, no need to update, we know the fields are the same
		o.repoId, addr, object.Checksum, object.Size, object.PhysicalAddress, object.Metadata)
	return err
}

func (o *DBRepoOperations) WriteCommit(addr string, commit *model.Commit) error {
	_, err := o.tx.Exec(`
		INSERT INTO commits (repository_id, address, tree, committer, message, creation_date, parents, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT ON CONSTRAINT commits_pkey
		DO NOTHING`, // since it's keyed by hash of content, no need to update, we know the fields are the same
		o.repoId, addr, commit.Tree, commit.Committer, commit.Message, commit.CreationDate, commit.Parents, commit.Metadata)
	return err
}

func (o *DBRepoOperations) WriteBranch(name string, branch *model.Branch) error {
	_, err := o.tx.Exec(`
		INSERT INTO branches (repository_id, id, commit_id, commit_root, workspace_root)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT ON CONSTRAINT branches_pkey
		DO UPDATE SET commit_id = $3, commit_root = $4, workspace_root = $5`,
		o.repoId, name, branch.CommitId, branch.CommitRoot, branch.WorkspaceRoot)
	return err
}

func (o *DBRepoOperations) DeleteBranch(name string) error {
	_, err := o.tx.Exec(`DELETE FROM branches WHERE repository_id = $1 AND id = $2`,
		o.repoId, name)
	return err
}

func (o *DBRepoOperations) WriteMultipartUpload(upload *model.MultipartUpload) error {
	_, err := o.tx.Exec(`
		INSERT INTO multipart_uploads (repository_id, upload_id, path, creation_date,physical_address)
		VALUES ($1, $2, $3, $4, $5)`,
		o.repoId, upload.UploadId, upload.Path, upload.CreationDate, upload.PhysicalAddress)
	return err
}

func (o *DBRepoOperations) WriteRepo(repo *model.Repo) error {
	_, err := o.tx.Exec(`
		INSERT INTO repositories (id, storage_namespace, creation_date, default_branch)
		VALUES ($1, $2, $3, $4)`,
		repo.Id, repo.StorageNamespace, repo.CreationDate, repo.DefaultBranch)
	return err
}

func (o *DBRepoOperations) DeleteMultipartUpload(uploadId string) error {
	_, err := o.tx.Exec(`
		DELETE FROM multipart_uploads
		WHERE repository_id = $1 AND upload_id = $2`,
		o.repoId, uploadId)
	return err
}
