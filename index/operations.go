package index

import (
	"versio-index/index/model"

	"github.com/golang/protobuf/proto"
)

func (q *readQuery) ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error) {
	// read the blob's hash addr
	data := q.get(q.workspace, branch, path).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	ent := &model.WorkspaceEntry{}
	err := proto.Unmarshal(data, ent)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return ent, nil
}

func (q *readQuery) ReadBranch(branch string) (*model.Branch, error) {
	// read branch attributes
	data := q.get(q.branches, branch).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	branchModel := &model.Branch{}
	err := proto.Unmarshal(data, branchModel)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return branchModel, nil
}

func (q *readQuery) ReadBlob(addr string) (*model.Blob, error) {
	blobData := q.get(q.blobs, addr).MustGet()
	if blobData == nil {
		return nil, ErrNotFound
	}
	blob := &model.Blob{}
	err := proto.Unmarshal(blobData, blob)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return blob, nil
}

func (q *readQuery) ReadTree(addr string) (*model.Tree, error) {
	data := q.get(q.trees, addr).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	tree := &model.Tree{}
	err := proto.Unmarshal(data, tree)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return tree, nil
}

func (q *readQuery) ReadCommit(addr string) (*model.Commit, error) {
	data := q.get(q.commits, addr).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	commit := &model.Commit{}
	err := proto.Unmarshal(data, commit)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return commit, nil
}

func (q *readQuery) ListEntries(addr string) ([]*model.Entry, error) {
	iter := q.rangePrefix(q.entries, addr)
	entries := make([]*model.Entry, 0)
	for iter.Advance() {
		entryData := iter.MustGet()
		entry := &model.Entry{}
		err := proto.Unmarshal(entryData.Value, entry)
		if err != nil {
			return nil, ErrIndexMalformed
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (q *readQuery) ReadEntry(treeAddress, entryType, name string) (*model.Entry, error) {
	data := q.get(q.entries, treeAddress, entryType, name).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	entry := &model.Entry{}
	err := proto.Unmarshal(data, entry)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return entry, nil
}

func (q *query) WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return ErrIndexMalformed
	}
	q.set(data, q.workspace, path)
	return nil
}

func (q *query) ClearWorkspace(branch string) {
	q.clearPrefix(q.workspace, q.repo.GetClientId(), q.repo.GetRepoId(), branch)
}

func (q *query) WriteTree(addr string, tree *model.Tree) error {
	data, err := proto.Marshal(tree)
	if err != nil {
		return err
	}
	q.set(data, q.trees, addr)
	return nil
}

func (q *query) WriteEntry(treeAddress, entryType, name string, entry *model.Entry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	q.set(data, q.entries, treeAddress, entryType, name)
	return nil
}

func (q *query) WriteBlob(addr string, blob *model.Blob) error {
	data, err := proto.Marshal(blob)
	if err != nil {
		return err
	}
	q.set(data, q.blobs, addr)
	return nil
}

func (q *query) WriteCommit(addr string, commit *model.Commit) error {
	data, err := proto.Marshal(commit)
	if err != nil {
		return err
	}
	q.set(data, q.commits, addr)
	return nil
}

func (q *query) WriteBranch(name string, branch *model.Branch) error {
	data, err := proto.Marshal(branch)
	if err != nil {
		return err
	}
	q.set(data, q.branches, name)
	return nil
}

func (q *query) DeleteBranch(name string) {
	q.delete(q.branches, name)
}
