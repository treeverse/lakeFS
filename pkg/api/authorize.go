package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/api/gen/models"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/permissions"
)

var ErrAuthorization = errors.New("authorization error")

func authorize(ctx context.Context, a auth.Service, user *models.User, permissions []permissions.Permission) error {
	authResp, err := a.Authorize(ctx, &auth.AuthorizationRequest{
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
