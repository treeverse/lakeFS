package index

import (
	"versio-index/index/model"

	"github.com/golang/protobuf/proto"
)

func (r *QueryReader) ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error) {
	// read the blob's hash addr
	data := r.get(r.query.workspace, branch, path).MustGet()
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

func (r *QueryReader) ReadBranch(branch string) (*model.Branch, error) {
	// read branch attributes
	data := r.get(r.query.branches, branch).MustGet()
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

func (r *QueryReader) ReadBlob(addr string) (*model.Blob, error) {
	blobData := r.get(r.query.blobs, addr).MustGet()
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

func (r *QueryReader) ReadTree(addr string) (*model.Tree, error) {
	data := r.get(r.query.trees, addr).MustGet()
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

func (r *QueryReader) ReadCommit(addr string) (*model.Commit, error) {
	data := r.get(r.query.commits, addr).MustGet()
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

func (r *QueryReader) ListEntries(addr string) ([]*model.Entry, error) {
	iter := r.rangePrefix(r.query.entries, addr)
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

func (r *QueryReader) ReadEntry(treeAddress, entryType, name string) (*model.Entry, error) {
	data := r.get(r.query.entries, treeAddress, entryType, name).MustGet()
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

func (w *QueryWriter) WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return ErrIndexMalformed
	}
	w.set(data, w.query.workspace, path)
	return nil
}

func (w *QueryWriter) WriteTree(addr string, tree *model.Tree) error {
	data, err := proto.Marshal(tree)
	if err != nil {
		return err
	}
	w.set(data, w.query.trees, addr)
	return nil
}

func (w *QueryWriter) WriteEntry(treeAddress, entryType, name string, entry *model.Entry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	w.set(data, w.query.entries, treeAddress, entryType, name)
	return nil
}

func (w *QueryWriter) WriteBlob(addr string, blob *model.Blob) error {
	data, err := proto.Marshal(blob)
	if err != nil {
		return err
	}
	w.set(data, w.query.blobs, addr)
	return nil
}

func (w *QueryWriter) WriteCommit(addr string, commit *model.Commit) error {
	data, err := proto.Marshal(commit)
	if err != nil {
		return err
	}
	w.set(data, w.query.commits, addr)
	return nil
}

func (w *QueryWriter) WriteBranch(name string, branch *model.Branch) error {
	data, err := proto.Marshal(branch)
	if err != nil {
		return err
	}
	w.set(data, w.query.branches, name)
	return nil
}

func (w *QueryWriter) DeleteBranch(name string) {
	w.delete(w.query.branches, name)
}
