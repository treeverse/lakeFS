package index

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/gateway/utils"

	"github.com/google/uuid"
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
	WithContext(ctx context.Context) MultipartManager
	Create(repoId, path string, createTime time.Time) (uploadId string, err error)
	UploadPart(repoId, path, uploadId string, partNumber int, part *model.MultipartUploadPart) error
	CopyPart(repoId, path, uploadId string, partNumber int, sourcePath, sourceBranch string, uploadTime time.Time) error
	Abort(repoId, path, uploadId string) error
	Complete(repoId, branch, path, uploadId string, parts []*model.MultipartUploadPartRequest, completionTime time.Time) (*model.Object, error)
}

type DBMultipartManager struct {
	db  store.Store
	ctx context.Context
}

func NewDBMultipartManager(db store.Store) *DBMultipartManager {
	return &DBMultipartManager{db: db, ctx: context.Background()}
}

func (m *DBMultipartManager) WithContext(ctx context.Context) MultipartManager {
	return &DBMultipartManager{
		db:  store.WithLogger(m.db, multipartLogger(ctx)),
		ctx: ctx,
	}
}

func multipartLogger(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx).WithFields(logging.Fields{"service_name": "multipart_manager"})
}

func (m *DBMultipartManager) log() logging.Logger {
	return multipartLogger(m.ctx)
}

func (m *DBMultipartManager) generateId() string {
	id := fmt.Sprintf("%x", []byte(uuidAsSlice()+uuidAsSlice()))
	return id
}

func uuidAsSlice() string {
	id := [16]byte(uuid.New())
	return string(id[:])
}

func (m *DBMultipartManager) Create(repoId, path string, createTime time.Time) (string, error) {
	uploadId, err := m.db.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {

		// generate 256KB of random bytes
		var uploadId string
		var err error
		if utils.IsPlayback() {
			uploadId = utils.GetUploadId()
		} else {
			uploadId = m.generateId()
		}

		// save it for this repo and path
		err = tx.WriteMultipartUpload(&model.MultipartUpload{
			Path:         path,
			Id:           uploadId,
			CreationDate: createTime,
		})
		if err != nil {
			m.log().WithError(err).Error("failed to create MPU")
		}
		return uploadId, err
	})
	return uploadId.(string), err
}

func (m *DBMultipartManager) UploadPart(repoId, path, uploadId string, partNumber int, part *model.MultipartUploadPart) error {
	_, err := m.db.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// verify upload and part number
		mpu, err := tx.ReadMultipartUpload(uploadId)
		if err != nil {
			m.log().WithError(err).Error("failed to read MPU")
			return nil, err
		}
		if !strings.EqualFold(mpu.Path, path) {
			return nil, errors.ErrMultipartPathMismatch
		}
		// validate part number is 1-10000
		if partNumber < MinPartInMultipartUpload || partNumber >= MaxPartsInMultipartUpload {
			return nil, errors.ErrMultipartInvalidPartNumber
		}
		err = tx.WriteMultipartUploadPart(uploadId, partNumber, part)
		if err != nil {
			m.log().WithError(err).WithFields(logging.Fields{
				"uploadId":   uploadId,
				"partNumber": partNumber,
				"part":       part,
			}).Error("failed to write MPU part")
		}
		return nil, err
	})
	return err
}

func (m *DBMultipartManager) CopyPart(repoId, path, uploadId string, partNumber int, sourcePath, sourceBranch string, uploadTime time.Time) error {
	_, err := m.db.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// verify upload and part number
		mpu, err := tx.ReadMultipartUpload(uploadId)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(mpu.Path, path) {
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
		tree := merkle.New(branch.CommitRoot)
		obj, err := tree.GetObject(tx, sourcePath)
		if err != nil {
			return nil, err
		}

		// copy it as MPU part
		err = tx.WriteMultipartUploadPart(uploadId, partNumber, &model.MultipartUploadPart{
			Blocks:       obj.Blocks,
			Checksum:     obj.Checksum,
			CreationDate: uploadTime,
			Size:         obj.Size,
		})
		if err != nil {
			m.log().WithError(err).Error("failed to write MPU part")
		}
		return nil, err
	})
	return err
}

func (m *DBMultipartManager) Abort(repoId, path, uploadId string) error {
	_, err := m.db.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// read it first
		mpu, err := tx.ReadMultipartUpload(uploadId)
		if err != nil {
			return nil, err
		}

		// ensure its for the correct path
		if !strings.EqualFold(mpu.Path, path) {
			return nil, errors.ErrMultipartPathMismatch
		}

		// delete mpu ID
		err = tx.DeleteMultipartUpload(uploadId)
		if err != nil {
			m.log().WithError(err).Error("failed to delete MPU")
		}
		return nil, err

	})
	return err
}

func (m *DBMultipartManager) Complete(repoId, branch, path, uploadId string, parts []*model.MultipartUploadPartRequest, completionTime time.Time) (*model.Object, error) {
	obj, err := m.db.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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
				m.log().WithError(err).Error("failed to read MPU part")
				return nil, err
			}
			if !strings.EqualFold(savedPart.Checksum, part.Etag) {
				return nil, errors.ErrMultipartInvalidPartETag
			}
			savedParts[i] = savedPart
		}

		var size int64
		blocks := make([]*model.Block, 0)
		blockIds := make([]string, 0)
		for _, part := range savedParts {
			for _, block := range part.Blocks {
				blocks = append(blocks, block)
				blockIds = append(blockIds, block.Address)
			}
			size += part.Size
		}

		// build and save the object
		obj := &model.Object{
			Blocks:   blocks,
			Checksum: ident.MultiHash(blockIds...),
			Size:     size,
		}
		err = tx.WriteObject(ident.Hash(obj), obj)
		if err != nil {
			m.log().WithError(err).Error("failed to write object")
			return nil, err
		}

		// write it to branch's workspace
		typ := model.EntryTypeObject
		p := pth.New(upload.Path, typ)
		upath := p.String()
		baseName := p.BaseName()
		id := ident.Hash(obj)
		err = tx.WriteToWorkspacePath(branch, p.ParentPath(), upath, &model.WorkspaceEntry{
			RepositoryId:      repoId,
			BranchId:          branch,
			ParentPath:        p.ParentPath(),
			Path:              upload.Path,
			EntryName:         &baseName,
			EntryAddress:      &id,
			EntryType:         &typ,
			EntryCreationDate: &completionTime,
			EntrySize:         &obj.Size,
			EntryChecksum:     &obj.Checksum,
		})
		if err != nil {
			m.log().WithError(err).Error("failed to write workspace entry")
			return nil, err
		}

		// remove MPU part entries for the MPU
		err = tx.DeleteMultipartUpload(uploadId)
		if err != nil {
			m.log().WithError(err).Error("failed to write delete MPU")
		}
		return obj, err
	})
	if err != nil {
		return nil, err
	}
	return obj.(*model.Object), nil
}
