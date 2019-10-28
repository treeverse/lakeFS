package operations

import (
	authmodel "versio-index/auth/model"
)

type DeleteObject struct{}

func (controller *DeleteObject) GetArn() string {
	return "versio:repos:::{bucket}"
}

func (controller *DeleteObject) GetIntent() authmodel.Permission_Intent {
	return authmodel.Permission_REPO_WRITE
}

func (controller *DeleteObject) Handle(o *PathOperation) {

}
