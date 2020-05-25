package store

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	indexerrors "github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/path"
	"text/template"
)

type RepoOperations interface {
	ReadRepo() (*model.Repo, error)
	LockWorkspace() error
	ListWorkspace(branch string) ([]*model.WorkspaceEntry, error)
	ListWorkspaceDirectory(branch, path, prefix, from string, amount int) ([]*model.WorkspaceEntry, bool, error)
	ListWorkspaceWithPrefix(branch, prefix, from string, amount int) ([]*model.WorkspaceEntry, bool, error)
	ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error)
	ListBranches(prefix string, amount int, after string) ([]*model.Branch, bool, error)
	ReadBranch(branch string) (*model.Branch, error)
	ReadObject(addr string) (*model.Object, error)
	ReadCommit(addr string) (*model.Commit, error)
	ListTree(addr, after string, results int) ([]*model.Entry, bool, error)
	ListTreeWithPrefix(addr, prefix, after string, results int, afterInclusive bool) ([]*model.Entry, bool, error)
	ReadTreeEntry(treeAddress, name string) (*model.Entry, error)

	// Multipart uploads
	ReadMultipartUpload(uploadId string) (*model.MultipartUpload, error)
	ListMultipartUploads() ([]*model.MultipartUpload, error)
	GetObjectDedup(DedupId string) (*model.ObjectDedup, error)
	DeleteWorkspacePath(branch, path, typ string) error
	WriteToWorkspacePath(branch, parentPath, path string, entry *model.WorkspaceEntry) error
	ClearWorkspace(branch string) error
	WriteTree(address string, entries []*model.Entry) error
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
func (o *DBRepoOperations) ListWorkspaceDirectory(branch, dir, prefix, from string, amount int) ([]*model.WorkspaceEntry, bool, error) {
	var entries []*model.WorkspaceEntry
	values := struct {
		AdditionalCondition    string
		AdditionalConditionDir string
		LimitStatement         string
		Sep                    string
	}{
		Sep: path.Separator,
	}
	if amount >= 0 {
		values.LimitStatement = fmt.Sprintf("LIMIT %d", amount+1)
	}
	args := []interface{}{o.repoId, branch, dir, prefix}
	if len(from) > 0 {
		values.AdditionalCondition = "AND path > $5"
		values.AdditionalConditionDir = fmt.Sprintf("AND LEFT(path, LENGTH($3) + STRPOS(SUBSTRING(path FROM LENGTH($3) + 1), '%s')) > $5", path.Separator)
		args = append(args, from)
	}
	buf := &bytes.Buffer{}
	tmpl, err := template.New("query").Parse(`SELECT * FROM (
					(SELECT parent_path, path, entry_name, entry_type, entry_size, entry_creation_date, entry_checksum, CASE WHEN tombstone THEN 1 ELSE 0 END AS tombstone_count
					FROM workspace_entries
    				WHERE repository_id = $1 AND branch_id = $2 AND parent_path = $3 AND entry_name LIKE $4 || '%' {{.AdditionalCondition}}
					ORDER BY 3 {{.LimitStatement}})
						UNION ALL
					(SELECT $3 as parent_path,
					   LEFT(path, LENGTH($3) + STRPOS(SUBSTRING(path FROM LENGTH($3) + 1), '{{.Sep}}')) AS path,
					   SUBSTRING(path FROM LENGTH($3) + 1 FOR STRPOS(SUBSTRING(path FROM length($3) + 1), '{{.Sep}}')) AS entry_name,
					   'tree' as entry_type, NULL, NULL, NULL,
					   CASE WHEN bool_and(tombstone) THEN COUNT(*) ELSE -1 END AS tombstone_count 
					FROM workspace_entries
					WHERE repository_id = $1 AND branch_id = $2 AND parent_path LIKE $3 || '%' AND parent_path <> $3 AND entry_name LIKE $4 || '%' {{.AdditionalConditionDir}}  
					GROUP BY 1,2,3,4 ORDER BY 3 {{.LimitStatement}})
				) t ORDER BY path {{.LimitStatement}}`)
	if err != nil {
		return nil, false, err
	}
	err = tmpl.ExecuteTemplate(buf, "query", values)
	if err != nil {
		return nil, false, err
	}
	err = o.tx.Select(&entries, buf.String(), args...)
	if err != nil {
		return nil, false, err
	}
	hasMore := false
	if amount >= 0 && len(entries) > amount {
		hasMore = true
		entries = entries[:amount]
	}
	return entries, hasMore, nil
}

func (o *DBRepoOperations) ListWorkspaceWithPrefix(branch, prefix, from string, amount int) ([]*model.WorkspaceEntry, bool, error) {
	var entries []*model.WorkspaceEntry
	additionalCondition := ""
	args := []interface{}{o.repoId, branch, prefix}
	if len(from) > 0 {
		additionalCondition = " AND path > $4"
		args = append(args, from)
	}
	limitClause := ""
	if amount >= 0 {
		limitClause = fmt.Sprintf("LIMIT %d", amount+1)
	}
	query := fmt.Sprintf(`SELECT *, CASE tombstone WHEN true THEN 1 ELSE 0 END AS tombstone_count FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2 AND path LIKE $3 || '%%' %s ORDER BY path %s`,
		additionalCondition, limitClause)
	err := o.tx.Select(&entries, query, args...)
	hasMore := false
	if amount >= 0 && len(entries) > amount {
		hasMore = true
		entries = entries[:amount]
	}
	return entries, hasMore, err
}

func (o *DBRepoOperations) ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error) {
	ent := &model.WorkspaceEntry{}
	err := o.tx.Get(ent, `SELECT * FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2 AND path = $3`,
		o.repoId, branch, path)
	if err != nil {
		return nil, err
	}
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

func (o *DBRepoOperations) ListTreeWithPrefix(addr, prefix, after string, amount int, afterInclusive bool) ([]*model.Entry, bool, error) {
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
		nameOperator := ">"
		if afterInclusive {
			nameOperator = ">="
		}
		if amount >= 0 {
			err = o.tx.Select(&entries, fmt.Sprintf(`SELECT * FROM entries WHERE repository_id = $1 AND parent_address = $2 AND name LIKE $3 AND name %s $4 ORDER BY name ASC LIMIT $5`,
				nameOperator), o.repoId, addr, db.Prefix(prefix), after, amount+1)
		} else {
			err = o.tx.Select(&entries, fmt.Sprintf(`SELECT * FROM entries WHERE repository_id = $1 AND parent_address = $2 AND name LIKE $3 AND name %s $4 ORDER BY name ASC`, nameOperator),
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
	return o.ListTreeWithPrefix(addr, "", after, results, false)
}

func (o *DBRepoOperations) ReadTreeEntry(treeAddress, name string) (*model.Entry, error) {
	entry := &model.Entry{}
	err := o.tx.Get(entry, `SELECT * FROM entries WHERE repository_id = $1 AND parent_address = $2 AND name = $3`,
		o.repoId, treeAddress, name)
	return entry, err
}

func (o *DBRepoOperations) GetObjectDedup(dedupId string) (*model.ObjectDedup, error) {
	m := &model.ObjectDedup{}
	err := o.tx.Get(m, `SELECT repository_id,encode(dedup_id,'hex') as dedup_id,physical_address FROM object_dedup WHERE repository_id = $1 AND dedup_id = decode($2,'hex')`,
		o.repoId, dedupId)
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

func (o *DBRepoOperations) DeleteWorkspacePath(branch, path, typ string) error {
	var query string
	if typ == model.EntryTypeObject {
		query = `DELETE FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2 AND path = $3`
	} else {
		query = `DELETE FROM workspace_entries WHERE repository_id = $1 AND branch_id = $2 AND parent_path LIKE $3 || '%'`
	}
	result, err := o.tx.Exec(query, o.repoId, branch, path)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return db.ErrNotFound
	}
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

func (o *DBRepoOperations) WriteObjectDedup(dedup *model.ObjectDedup) error {

	_, err := o.tx.Exec(`INSERT INTO object_dedup  (repository_id, dedup_id,physical_address) values ($1,decode($2,'hex'),$3)`,
		o.repoId, dedup.DedupId, dedup.PhysicalAddress)
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
			INSERT INTO entries (repository_id, parent_address, name, address, type, creation_date, size, checksum, object_count)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT ON CONSTRAINT entries_pkey
			DO NOTHING`,
			o.repoId, address, entry.Name, entry.Address, entry.EntryType, entry.CreationDate, entry.Size, entry.Checksum, entry.ObjectCount)
		if err != nil {
			return err
		}
	}
	return nil
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
		INSERT INTO branches (repository_id, id, commit_id, commit_root)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT ON CONSTRAINT branches_pkey
		DO UPDATE SET commit_id = $3, commit_root = $4`,
		o.repoId, name, branch.CommitId, branch.CommitRoot)
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
