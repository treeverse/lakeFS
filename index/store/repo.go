package store

import (
	"database/sql"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"strings"

	"golang.org/x/xerrors"

	"github.com/treeverse/lakefs/ident"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/index/model"
)

const MaxResultsAllowed = 1000

type RepoOperations interface {
	ReadRepo() (*model.Repo, error)
	ListWorkspace(branch string) ([]*model.WorkspaceEntry, error)
	ListWorkspaceAsDiff(branch string) (model.Differences, error)
	ListWorkspaceWithPrefix(branch, parentPath, after string, amount int) ([]*model.WorkspaceEntry, error)
	ListWorkspaceDirectory(branch, parentPath, after string, amount int) ([]*model.WorkspaceEntry, error)
	ListTreeAndWorkspaceDirectory(branch, path string) ([]*model.SearchResultEntry, error)
	CascadeDirectoryDeletion(branch, deletedPath string) error
	ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error)
	ListBranches(prefix string, amount int, after string) ([]*model.Branch, bool, error)
	ReadBranch(branch string) (*model.Branch, error)
	ReadRoot(addr string) (*model.Root, error)
	ReadObject(addr string) (*model.Object, error)
	ReadCommit(addr string) (*model.Commit, error)
	ListTree(addr, after string, results int) ([]*model.Entry, bool, error)
	ListTreeWithPrefix(addr, prefix, after string, results int) ([]*model.Entry, bool, error)
	ReadTreeEntry(treeAddress, name string) (*model.Entry, error)
	DeleteWorkspacePath(branch, path string) error
	WriteToWorkspacePath(branch, parentPath, path string, entries []*model.WorkspaceEntry) error
	ClearWorkspace(branch string) error
	WriteTree(address string, entries []*model.Entry) error
	WriteRoot(address string, root *model.Root) error
	WriteObject(addr string, object *model.Object) error
	WriteCommit(addr string, commit *model.Commit) error
	WriteBranch(name string, branch *model.Branch) error
	DeleteBranch(name string) error
	WriteRepo(repo *model.Repo) error

	// Multipart uploads
	ReadMultipartUpload(uploadId string) (*model.MultipartUpload, error)
	ReadMultipartUploadPart(uploadId string, partNumber int) (*model.MultipartUploadPart, error)
	ListMultipartUploads() ([]*model.MultipartUpload, error)
	ListMultipartUploadParts(uploadId string) ([]*model.MultipartUploadPart, error)
	WriteMultipartUpload(upload *model.MultipartUpload) error
	WriteMultipartUploadPart(uploadId string, partNumber int, part *model.MultipartUploadPart) error
	DeleteMultipartUpload(uploadId string) error
	DeleteMultipartUploadParts(uploadId string) error
}

func (o *DBRepoOperations) ListTreeAndWorkspaceDirectory(branch string, path string) ([]*model.SearchResultEntry, error) {
	var entries []*model.SearchResultEntry
	err := o.tx.Select(
		&entries,
		`SELECT	COALESCE(wse.parent_path, ewp.parent_path) || COALESCE(wse.entry_name, ewp.name) AS name,
          COALESCE(wse.entry_type, ewp.type) AS type,
          COALESCE(wse.entry_size, ewp.size) AS size,
          COALESCE(wse.entry_creation_date, ewp.creation_date) AS creation_date,
          COALESCE(wse.entry_checksum, ewp.checksum) AS checksum
		  FROM workspace_entries wse FULL OUTER JOIN (SELECT unnest(branches) as branch, * FROM entries_with_path) ewp ON wse.branch_id = ewp.branch
		    AND wse.parent_path = ewp.parent_path AND wse.entry_name = ewp.name AND wse.repository_id = ewp.repository_id
    	  WHERE COALESCE(wse.repository_id, ewp.repository_id) = $1 
    	    AND COALESCE(wse.branch_id, ewp.branch) = $2
    		AND wse.tombstone IS NOT TRUE AND (wse.parent_path = $3 OR ewp.parent_path = $3) ORDER BY name`,
		o.repoId, branch, path)
	return entries, err
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

func (o *DBRepoOperations) ListWorkspace(branch string) ([]*model.WorkspaceEntry, error) {
	var entries []*model.WorkspaceEntry
	err := o.tx.Select(
		&entries,
		`SELECT * FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2 AND entry_type = 'object'`,
		o.repoId, branch)
	return entries, err
}
func (o *DBRepoOperations) ListWorkspaceAsDiff(branch string) (model.Differences, error) {
	var entries model.Differences
	err := o.tx.Select(
		&entries,
		fmt.Sprintf(`SELECT diff_path AS path, CASE object_path WHEN diff_path THEN 'object' ELSE 'tree' END AS entrY_type, %d as diff_direction,
								CASE diff_type
									WHEN 'ADDED' THEN %d
									WHEN 'DELETED' THEN %d
								 	WHEN 'CHANGED' THEN %d END AS diff_type
								FROM workspace_diff WHERE repository_id = $1 AND branch_id = $2`,
			model.DifferenceDirectionRight, model.DifferenceTypeAdded, model.DifferenceTypeRemoved, model.DifferenceTypeChanged),
		o.repoId, branch)
	return entries, err
}

//}func (o *DBRepoOperations) ListWorkspaceAsDiff(branch string) (model.Differences, error) {
//	var entries model.Differences
//	err := o.tx.Select(
//		&entries,
//		fmt.Sprintf(`SELECT	wse.path AS path,
//                        wse.entry_type AS entry_type,
//						'%d' AS diff_direction,
//						CASE
//							WHEN ewp.parent_path IS NULL THEN %d
//							WHEN wse.tombstone = true THEN %d
//							WHEN ewp.parent_path IS NOT NULL THEN %d
//						END as diff_type
//                    FROM workspace_entries wse
//						LEFT JOIN (SELECT unnest(branches) as branch, * FROM entries_with_path) ewp
//							ON wse.branch_id = ewp.branch
//                        		AND wse.parent_path = ewp.parent_path
//								AND wse.entry_name = ewp.name
//								AND wse.repository_id = ewp.repository_id
//					WHERE wse.repository_id = $1 AND wse.branch_id = $2`, model.DifferenceDirectionRight, model.DifferenceTypeAdded, model.DifferenceTypeRemoved, model.DifferenceTypeChanged),
//		o.repoId, branch)
//	return entries, err
//}

func (o *DBRepoOperations) ListWorkspaceWithPrefix(branch, prefix, after string, amount int) ([]*model.WorkspaceEntry, error) {
	if amount == 0 {
		amount = MaxResultsAllowed
	}
	sb := sqlbuilder.PostgreSQL.NewSelectBuilder()
	sb.Select("repository_id", "branch_id", "parent_path", "path", "entry_name", "entry_size", "entry_type", "tombstone")
	sb.From("workspace_entries")
	sb.Where(sb.Equal("repository_id", o.repoId))
	sb.Where(sb.Equal("branch_id", branch))
	sb.Where(sb.Like("path", prefix+"%"))
	sb.OrderBy("path ASC")
	if len(after) > 0 {
		sb.Where(sb.GreaterThan("path", after))
	}
	sb.Limit(amount)
	query, args := sb.Build()
	query = o.tx.Rebind(query)
	var entries []*model.WorkspaceEntry
	err := o.tx.Select(&entries, query, args...)
	return entries, err
}

func (o *DBRepoOperations) ListWorkspaceDirectory(branch, parentPath, after string, amount int) ([]*model.WorkspaceEntry, error) {
	if amount == 0 {
		amount = MaxResultsAllowed
	}
	fsb := sqlbuilder.PostgreSQL.NewSelectBuilder()
	fsb.Select("repository_id", "branch_id", "parent_path", "path", "entry_name", "entry_size", "entry_type", "tombstone")
	fsb.From("workspace_entries")
	fsb.Where(fsb.Equal("repository_id", o.repoId))
	fsb.Where(fsb.Equal("branch_id", branch))
	fsb.Where(fsb.Equal("parent_path", parentPath))
	fsb.OrderBy("path ASC")
	if len(after) > 0 {
		fsb.Where(fsb.GreaterThan("path", after))
	}
	fsb.Limit(amount)
	dsb := sqlbuilder.PostgreSQL.NewSelectBuilder()
	separatorCount := strings.Count(parentPath, "/")
	dirEntryNameSql := fmt.Sprintf("(string_to_array(path, '/'))[%d]", separatorCount+1)
	newPathSql := fmt.Sprintf("array_to_string((string_to_array(path, '/'))[:%d], '/') || '/'", separatorCount+1)
	parentPathSql := fmt.Sprintf("array_to_string((string_to_array(path, '/'))[:%d], '/') || '/'", separatorCount)
	dsb.Select(
		"repository_id", "branch_id",
		dsb.As(parentPathSql, "new_parent_path"),
		dsb.As(newPathSql, "new_path"),
		dsb.As(dirEntryNameSql, "new_entry_name"),
		dsb.As("SUM(entry_size)", "entry_size"),
		dsb.As("'tree'", "entry_type"), "not bool_or(not tombstone) AS tombstone")

	dsb.From("workspace_entries")
	dsb.Where(
		dsb.Equal("repository_id", o.repoId),
		dsb.Equal("branch_id", branch),
		dsb.Like("parent_path", parentPath+"%"),
		dsb.NotEqual("parent_path", parentPath))
	if len(after) > 0 {
		dsb.Where(dsb.GreaterThan(newPathSql, after))
	}
	dsb.GroupBy("repository_id", "branch_id", "new_parent_path", "new_path", "new_entry_name")
	dsb.OrderBy("new_path ASC")
	dsb.Limit(amount)

	var entries []*model.WorkspaceEntry
	// language=sql
	query, args := sqlbuilder.Buildf("SELECT * FROM ((%v) UNION ALL (%v)) t ORDER BY path LIMIT %s", fsb, dsb, amount).Build()
	query = o.tx.Rebind(query)
	err := o.tx.Select(&entries, query, args...)
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
	if xerrors.Is(err, db.ErrNotFound) {
		err = errors.ErrBranchNotFound
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
	err := o.tx.Get(obj, `SELECT * FROM objects WHERE repository_id = $1 AND address = $2`, o.repoId, addr)
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

func (o *DBRepoOperations) ReadMultipartUpload(uploadId string) (*model.MultipartUpload, error) {
	m := &model.MultipartUpload{}
	err := o.tx.Get(m, `SELECT * FROM multipart_uploads WHERE repository_id = $1 AND id = $2`,
		o.repoId, uploadId)
	return m, err
}

func (o *DBRepoOperations) ReadMultipartUploadPart(uploadId string, partNumber int) (*model.MultipartUploadPart, error) {
	m := &model.MultipartUploadPart{}
	err := o.tx.Get(m, `SELECT * FROM multipart_upload_parts WHERE repository_id = $1 AND upload_id = $2 AND part_number = $3`,
		o.repoId, uploadId, partNumber)
	return m, err
}

func (o *DBRepoOperations) ListMultipartUploads() ([]*model.MultipartUpload, error) {
	var mpus []*model.MultipartUpload
	err := o.tx.Select(&mpus, `SELECT * FROM multipart_uploads WHERE repository_id = $1`, o.repoId)
	return mpus, err
}

func (o *DBRepoOperations) ListMultipartUploadParts(uploadId string) ([]*model.MultipartUploadPart, error) {
	var parts []*model.MultipartUploadPart
	err := o.tx.Select(&parts, `SELECT * FROM multipart_upload_parts WHERE repository_id = $1 AND upload_id = $2`,
		o.repoId, uploadId)
	return parts, err
}

func (o *DBRepoOperations) DeleteWorkspacePath(branch, path string) error {
	_, err := o.tx.Exec(`DELETE FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2 AND path = $3`,
		o.repoId, branch, path)
	return err
}

func (o *DBRepoOperations) WriteToWorkspacePath(branch, parentPath, path string, entries []*model.WorkspaceEntry) error {
	sqlTemplate := `
		INSERT INTO workspace_entries (repository_id, branch_id, path, parent_path, 
			entry_name, entry_address, entry_type, entry_creation_date, entry_size, entry_checksum, tombstone)
		VALUES %s
		ON CONFLICT ON CONSTRAINT workspace_entries_pkey
		DO UPDATE SET parent_path = excluded.parent_path, entry_name = excluded.entry_name, entry_address = excluded.entry_address, entry_type = excluded.entry_type, entry_creation_date = excluded.entry_creation_date,
		entry_size = excluded.entry_size, entry_checksum = excluded.entry_checksum, tombstone = (excluded.tombstone AND (SELECT NOT bool_or(NOT tombstone) FROM workspace_entries hlpr WHERE hlpr.parent_path=excluded.parent_path AND hlpr.path <> excluded.path))`
	var vals []interface{}
	valsStr := ""
	idx := 1
	for _, entry := range entries {
		valsStr += fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d),", idx, idx+1, idx+2, idx+3, idx+4, idx+5, idx+6, idx+7, idx+8, idx+9, idx+10)
		idx += 11
		vals = append(vals, o.repoId, branch, entry.Path, entry.ParentPath, entry.EntryName, entry.EntryAddress, entry.EntryType,
			entry.EntryCreationDate, entry.EntrySize, entry.EntryChecksum, entry.Tombstone)
	}
	valsStr = strings.TrimSuffix(valsStr, ",")
	_, err := o.tx.Exec(fmt.Sprintf(sqlTemplate, valsStr), vals...)
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
	_, err := o.tx.Exec(`
		INSERT INTO objects (repository_id, address, checksum, size, blocks, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT ON CONSTRAINT objects_pkey
		DO NOTHING`, // since it's keyed by hash of content, no need to update, we know the fields are the same
		o.repoId, addr, object.Checksum, object.Size, object.Blocks, object.Metadata)
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
		INSERT INTO multipart_uploads (repository_id, id, path, creation_date)
		VALUES ($1, $2, $3, $4)`,
		o.repoId, upload.Id, upload.Path, upload.CreationDate)
	return err
}

func (o *DBRepoOperations) WriteRepo(repo *model.Repo) error {
	_, err := o.tx.Exec(`
		INSERT INTO repositories (id, storage_namespace, creation_date, default_branch)
		VALUES ($1, $2, $3, $4)`,
		repo.Id, repo.StorageNamespace, repo.CreationDate, repo.DefaultBranch)
	return err
}

func (o *DBRepoOperations) WriteMultipartUploadPart(uploadId string, partNumber int, part *model.MultipartUploadPart) error {
	_, err := o.tx.Exec(`
		INSERT INTO multipart_upload_parts (repository_id, upload_id, part_number, checksum, creation_date, size, blocks)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		o.repoId, uploadId, partNumber, part.Checksum, part.CreationDate, part.Size, part.Blocks)
	return err
}

func (o *DBRepoOperations) DeleteMultipartUpload(uploadId string) error {
	_, err := o.tx.Exec(`
		DELETE FROM multipart_upload_parts
		WHERE repository_id = $1 AND upload_id = $2`,
		o.repoId, uploadId)
	if err != nil {
		return err
	}
	_, err = o.tx.Exec(`
		DELETE FROM multipart_uploads
		WHERE repository_id = $1 AND id = $2`,
		o.repoId, uploadId)
	return err
}

func (o *DBRepoOperations) DeleteMultipartUploadParts(uploadId string) error {
	_, err := o.tx.Exec(`
		DELETE FROM multipart_upload_parts
		WHERE repository_id = $1 AND upload_id = $2`,
		o.repoId, uploadId)
	return err
}

func (o *DBRepoOperations) CascadeDirectoryDeletion(branch, deletedPathParent string) error {
	_, err := o.tx.Exec(`UPDATE workspace_entries we SET tombstone = 0 = (SELECT COUNT(*)
									FROM workspace_entries we2
									FULL OUTER JOIN (SELECT unnest(branches) as branch, * FROM entries_with_path) ewp
										ON we2.branch_id = ewp.branch
											AND we2.parent_path = ewp.parent_path
											AND we2.entry_name = ewp.name
											AND we2.repository_id = ewp.repository_id
										WHERE COALESCE(we2.repository_id, ewp.repository_id) = $1 AND COALESCE(we2.branch_id, ewp.branch) = $2
											AND COALESCE(we2.entry_type, ewp.type) = $3 AND we2.tombstone IS NOT TRUE
											AND COALESCE(we2.parent_path, ewp.parent_path) LIKE we.path || '%') 
								WHERE repository_id = $1 AND branch_id = $2 AND entry_type = $4 AND $5 LIKE path || '%'`,
		o.repoId, branch, model.EntryTypeObject, model.EntryTypeTree, deletedPathParent)
	return err
}
