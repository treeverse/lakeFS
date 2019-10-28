package operations

import (
	authmodel "versio-index/auth/model"
)

type DeleteBucket struct{}

func (controller *DeleteBucket) GetArn() string {
	return "versio:repos:::*"
}

func (controller *DeleteBucket) GetIntent() authmodel.Permission_Intent {
	return authmodel.Permission_ACCOUNT_ADMIN
}

func (controller *DeleteBucket) Handle(o *RepoOperation) {

}
