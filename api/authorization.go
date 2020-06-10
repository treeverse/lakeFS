package api

import (
	"fmt"

	"github.com/treeverse/lakefs/auth"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/permissions"
)

func authorize(a auth.Service, user *models.User, action permissions.Permission) error {
	authResp, err := a.Authorize(&auth.AuthorizationRequest{
		UserDisplayName: user.ID, Action: action.Action, Resource: action.Resource})
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
