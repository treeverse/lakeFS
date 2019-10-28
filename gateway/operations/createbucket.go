package operations

import (
	"net/http"
	authmodel "versio-index/auth/model"
	"versio-index/gateway/errors"
	"versio-index/index"
)

type CreateBucket struct{}

func (controller *CreateBucket) GetArn() string {
	return "versio:repos:::*"
}

func (controller *CreateBucket) GetIntent() authmodel.Permission_Intent {
	return authmodel.Permission_ACCOUNT_ADMIN
}

func (controller *CreateBucket) Handle(o *RepoOperation) {
	res := o.ResponseWriter
	err := o.Index.CreateRepo(o.ClientId, o.Repo, index.DefaultBranch)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	res.WriteHeader(http.StatusCreated)
}
