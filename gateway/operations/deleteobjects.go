package operations

import (
	authmodel "versio-index/auth/model"
)

type DeleteObjects struct{}

func (controller *DeleteObjects) GetArn() string {
	return "versio:repos:::{bucket}"
}

func (controller *DeleteObjects) GetIntent() authmodel.Permission_Intent {
	return authmodel.Permission_REPO_WRITE
}

func (controller *DeleteObjects) Handle(o *RepoOperation) {

}
