package handlers

import (
	"fmt"

	"github.com/treeverse/lakefs/auth"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/permissions"
)

func repoArn(repoId string) string {
	return fmt.Sprintf("arn:treeverse:repos:::%s", repoId)
}

func authorize(s ServerContext, user *models.User, perm permissions.Permission, arn string) error {
	authResp, err := s.GetAuthService().Authorize(&auth.AuthorizationRequest{
		UserID: user.ID, Permission: perm, SubjectARN: arn})
	if err != nil {
		return fmt.Errorf("authorization error")
	}

	if authResp.Error != nil {
		return fmt.Errorf("authorization error: %s", authResp.Error)
	}

	if !authResp.Allowed {
		return fmt.Errorf("authorization error: access denied")
	}
	return nil
}
