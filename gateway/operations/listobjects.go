package operations

import (
	authmodel "versio-index/auth/model"
)

type ListObjects struct{}

func (controller *ListObjects) GetArn() string {
	return "versio:repos:::{bucket}"
}

func (controller *ListObjects) GetIntent() authmodel.Permission_Intent {
	return authmodel.Permission_REPO_READ
}

func (controller *ListObjects) Handle(o *RepoOperation) {

}
