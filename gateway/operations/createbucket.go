package operations

import (
	"net/http"

	"golang.org/x/xerrors"

	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/utils"
	"github.com/treeverse/lakefs/index"
	ierrors "github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/permissions"
)

type CreateBucket struct{}

func (controller *CreateBucket) GetArn() string {
	return "arn:treeverse:repos:::*"
}

func (controller *CreateBucket) GetPermission() permissions.Permission {
	return permissions.ManageRepos
}

func (controller *CreateBucket) Handle(o *AuthenticatedOperation) {
	res := o.ResponseWriter
	repoId := utils.GetRepo(o.Request)

	err := o.Index.CreateRepo(repoId, index.DefaultBranch)
	if err != nil {
		if xerrors.Is(err, ierrors.ErrInvalid) {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidBucketName))
		} else {
			o.Log().WithField("repo", repoId).WithError(err).Error("failed to create repo")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		}

		return
	}
	o.Log().WithField("repo", repoId).Info("repo created successfully")
	res.WriteHeader(http.StatusCreated)
}
