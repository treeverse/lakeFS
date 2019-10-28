package operations

import (
	authmodel "versio-index/auth/model"
	"versio-index/db"
	"versio-index/gateway/errors"

	"golang.org/x/xerrors"
)

type HeadBucket struct{}

func (controller *HeadBucket) GetArn() string {
	return "versio:repos:::{bucket}"
}

func (controller *HeadBucket) GetIntent() authmodel.Permission_Intent {
	return authmodel.Permission_REPO_READ
}

func (controller *HeadBucket) Handle(o *RepoOperation) {
	_, err := o.Index.GetRepo(o.ClientId, o.Repo)
	if xerrors.Is(err, db.ErrNotFound) {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchBucket))
		return
	} else if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
}
