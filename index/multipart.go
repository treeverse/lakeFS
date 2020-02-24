package index

import (
	"crypto/rand"
	"strings"
	"time"

	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/index/model"
	pth "github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/index/store"
)

const (
	MaxPartsInMultipartUpload = 10000
	MinPartInMultipartUpload  = 1
)

type MultipartManager interface {
	Create(repoId, path string, createTime time.Time) (uploadId string, err error)
	UploadPart(repoId, path, uploadId string, partNumber int, part *model.MultipartUploadPart) error
	CopyPart(repoId, path, uploadId string, partNumber int, sourcePath, sourceBranch string, uploadTime time.Time) error
	Abort(repoId, path, uploadId string) error
	Complete(repoId, branch, path, uploadId string, parts []*model.MultipartUploadPartRequest, completionTime time.Time) (*model.Object, error)
}

type KVMultipartManager struct {
	kv store.Store
}

func NewKVMultipartManager(kv store.Store) *KVMultipartManager {
	return &KVMultipartManager{kv}
}

func (m *KVMultipartManager) generateId() (string, error) {
	b := make([]byte, 1024*256) // generate a random 256k slice of bytes
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return ident.Bytes(b), nil
}

func (m *KVMultipartManager) Create(repoId, path string, createTime time.Time) (string, error) {
	uploadId, err := m.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {

		// generate 256KB of random bytes
		uploadId, err := m.generateId()
		if err != nil {
			return uploadId, err
		}

		// save it for this repo and path
		err = tx.WriteMultipartUpload(&model.MultipartUpload{
			Path:      path,
			Id:        uploadId,
			Timestamp: createTime.Unix(),
		})
		return uploadId, err
	})
	return uploadId.(string), err
}

func (m *KVMultipartManager) UploadPart(repoId, path, uploadId string, partNumber int, part *model.MultipartUploadPart) error {
	_, err := m.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// verify upload and part number
		mpu, err := tx.ReadMultipartUpload(uploadId)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(mpu.GetPath(), path) {
			return nil, errors.ErrMultipartPathMismatch
		}
		// validate part number is 1-10000
		if partNumber < MinPartInMultipartUpload || partNumber >= MaxPartsInMultipartUpload {
			return nil, errors.ErrMultipartInvalidPartNumber
		}
		err = tx.WriteMultipartUploadPart(uploadId, partNumber, part)
		return nil, err
	})
	return err
}

func (m *KVMultipartManager) CopyPart(repoId, path, uploadId string, partNumber int, sourcePath, sourceBranch string, uploadTime time.Time) error {
	_, err := m.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// verify upload and part number
		mpu, err := tx.ReadMultipartUpload(uploadId)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(mpu.GetPath(), path) {
			return nil, errors.ErrMultipartPathMismatch
		}
		// validate part number is 1-10000
		if partNumber < MinPartInMultipartUpload || partNumber >= MaxPartsInMultipartUpload {
			return nil, errors.ErrMultipartInvalidPartNumber
		}
		// read source branch and addr
		branch, err := tx.ReadBranch(sourceBranch)
		if err != nil {
			return nil, err
		}

		// read root tree and traverse to path
		m := merkle.New(branch.GetCommitRoot())
		obj, err := m.GetObject(tx, sourcePath)
		if err != nil {
			return nil, err
		}

		// copy it as MPU part
		err = tx.WriteMultipartUploadPart(uploadId, partNumber, &model.MultipartUploadPart{
			Blocks:    obj.GetBlocks(),
			Checksum:  obj.GetChecksum(),
			Timestamp: uploadTime.Unix(),
			Size:      obj.GetSize(),
		})
		return nil, err
	})
	return err
}

func (m *KVMultipartManager) Abort(repoId, path, uploadId string) error {
	_, err := m.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// read it first
		mpu, err := tx.ReadMultipartUpload(uploadId)
		if err != nil {
			return nil, err
		}

		// ensure its for the correct path
		if !strings.EqualFold(mpu.GetPath(), path) {
			return nil, errors.ErrMultipartPathMismatch
		}

		// delete all part references
		err = tx.DeleteMultipartUploadParts(uploadId)
		if err != nil {
			return nil, err
		}
		// delete mpu ID
		err = tx.DeleteMultipartUpload(uploadId, mpu.GetPath())
		return nil, err

	})
	return err
}

func (m *KVMultipartManager) Complete(repoId, branch, path, uploadId string, parts []*model.MultipartUploadPartRequest, completionTime time.Time) (*model.Object, error) {
	obj, err := m.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		var err error

		// create new object in the current workspace for the given branch
		upload, err := tx.ReadMultipartUpload(uploadId)
		if err != nil {
			return nil, err
		}

		// compare requested parts with saved parts
		savedParts := make([]*model.MultipartUploadPart, len(parts))
		for i, part := range parts {
			// TODO: probably cheaper to read all MPU parts together instead of one by one, as most requests will complete with all uploaded parts anyway
			savedPart, err := tx.ReadMultipartUploadPart(uploadId, int(part.PartNumber))
			if err != nil {
				return nil, err
			}
			if !strings.EqualFold(savedPart.GetChecksum(), part.Etag) {
				return nil, errors.ErrMultipartInvalidPartETag
			}
			savedParts[i] = savedPart
		}

		var size int64
		blocks := make([]*model.Block, 0)
		blockIds := make([]string, 0)
		for _, part := range savedParts {
			for _, block := range part.GetBlocks() {
				blocks = append(blocks, block)
				blockIds = append(blockIds, block.GetAddress())
			}
			size += part.GetSize()
		}

		// build and save the object
		obj := &model.Object{
			Blocks:   blocks,
			Checksum: ident.MultiHash(blockIds...),
			Size:     size,
		}
		err = tx.WriteObject(ident.Hash(obj), obj)
		if err != nil {
			return nil, err
		}

		// write it to branch's workspace
		p := pth.New(upload.GetPath())
		upath := p.String()
		err = tx.WriteToWorkspacePath(branch, upath, &model.WorkspaceEntry{
			Path: upload.GetPath(),
			Entry: &model.Entry{
				Name:      p.Basename(),
				Address:   ident.Hash(obj),
				Type:      model.Entry_OBJECT,
				Timestamp: completionTime.Unix(),
				Size:      obj.GetSize(),
				Checksum:  obj.GetChecksum(),
			},
		})
		if err != nil {
			return nil, err
		}

		// remove MPU entry
		err = tx.DeleteMultipartUploadParts(uploadId)
		if err != nil {
			return nil, err
		}

		// remove MPU part entries for the MPU
		err = tx.DeleteMultipartUpload(uploadId, upload.GetPath())
		return obj, err
	})
	if err != nil {
		return nil, err
	}
	return obj.(*model.Object), nil
}
