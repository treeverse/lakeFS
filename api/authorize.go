package api

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/permissions"
)

var ErrAuthorization = errors.New("authorization error")

func authorize(a auth.Service, user *models.User, permissions []permissions.Permission) error {
	authResp, err := a.Authorize(&auth.AuthorizationRequest{
		Username:            user.ID,
		RequiredPermissions: permissions,
	})
	if err != nil {
		return ErrAuthorization
	}

	if authResp.Error != nil {
		return fmt.Errorf("authorization error: %w", authResp.Error)
	}

	if !authResp.Allowed {
		return fmt.Errorf("%w: access denied", ErrAuthorization)
	}
	return nil
}
