package operations

import (
	"treeverse-lake/db"
	"treeverse-lake/gateway/errors"
	"treeverse-lake/gateway/permissions"

	"golang.org/x/xerrors"
)

type HeadBucket struct{}

func (controller *HeadBucket) GetArn() string {
	return "arn:treeverse:repos:::{bucket}"
}

func (controller *HeadBucket) GetPermission() string {
	return permissions.PermissionReadRepo
}

func (controller *HeadBucket) Handle(o *RepoOperation) {
	_, err := o.Index.GetRepo(o.Repo)
	if xerrors.Is(err, db.ErrNotFound) {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchBucket))
		return
	} else if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
}
