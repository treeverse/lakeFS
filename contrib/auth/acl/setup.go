package acl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
)

const (
	AdminsGroup  = "Admins"
	SupersGroup  = "Supers"
	WritersGroup = "Writers"
	ReadersGroup = "Readers"
)

func CreateACLBaseGroups(ctx context.Context, authService auth.Service, ts time.Time) error {
	if _, err := authService.CreateGroup(ctx, &model.Group{CreatedAt: ts, DisplayName: AdminsGroup}); err != nil {
		return fmt.Errorf("setup: create base ACL group %s: %w", AdminsGroup, err)
	}
	if err := WriteGroupACL(ctx, authService, AdminsGroup, model.ACL{Permission: AdminPermission}, ts, false); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	if _, err := authService.CreateGroup(ctx, &model.Group{CreatedAt: ts, DisplayName: SupersGroup}); err != nil {
		return fmt.Errorf("setup: create base ACL group %s: %w", SupersGroup, err)
	}
	if err := WriteGroupACL(ctx, authService, SupersGroup, model.ACL{Permission: SuperPermission}, ts, false); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	if _, err := authService.CreateGroup(ctx, &model.Group{CreatedAt: ts, DisplayName: WritersGroup}); err != nil {
		return fmt.Errorf("setup: create base ACL group %s: %w", WritersGroup, err)
	}
	if err := WriteGroupACL(ctx, authService, WritersGroup, model.ACL{Permission: WritePermission}, ts, false); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	if _, err := authService.CreateGroup(ctx, &model.Group{CreatedAt: ts, DisplayName: ReadersGroup}); err != nil {
		return fmt.Errorf("create base ACL group %s: %w", ReadersGroup, err)
	}
	if err := WriteGroupACL(ctx, authService, ReadersGroup, model.ACL{Permission: ReadPermission}, ts, false); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	return nil
}

func SetupACLServer(ctx context.Context, authService *AuthService) error {
	t := time.Now()
	initialized, err := IsInitialized(ctx, authService)
	if err != nil || initialized {
		// return on error or if already initialized
		return err
	}

	if err = CreateACLBaseGroups(ctx, authService, t); err != nil {
		return err
	}

	return authService.updateSetupTimestamp(ctx, t)
}

func IsInitialized(ctx context.Context, authService *AuthService) (bool, error) {
	setupTimestamp, err := authService.getSetupTimestamp(ctx)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return !setupTimestamp.IsZero(), nil
}
