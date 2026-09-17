package setup

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

func AddAdminUser(ctx context.Context, authService auth.Service, user *model.SuperuserConfiguration) (*model.Credential, error) {
	// create admin user
	user.Source = "internal"
	_, err := authService.CreateUser(ctx, &user.User)
	if err != nil {
		return nil, fmt.Errorf("create user - %w", err)
	}
	defer func() {
		// delete admin on any error to avoid partial setup
		if err != nil {
			logger := logging.ContextUnavailable()
			logger.WithError(err).Warn("Failed to create admin user, deleting user")
			if delUserErr := authService.DeleteUser(ctx, user.Username); delUserErr != nil {
				logger.WithError(delUserErr).Error("Failed to delete user")
			}
		}
	}()

	var creds *model.Credential
	if user.AccessKeyID == "" {
		// Generate and return a key pair
		creds, err = authService.CreateCredentials(ctx, user.Username)
		if err != nil {
			return nil, fmt.Errorf("create credentials for %s: %w", user.Username, err)
		}
	} else {
		creds, err = authService.AddCredentials(ctx, user.Username, user.AccessKeyID, user.SecretAccessKey)
		if err != nil {
			return nil, fmt.Errorf("add credentials for %s: %w", user.Username, err)
		}
	}
	return creds, nil
}

func CreateInitialAdminUser(ctx context.Context, authService auth.Service, metadataManger auth.MetadataManager, username string) (*model.Credential, error) {
	return CreateInitialAdminUserWithKeys(ctx, authService, metadataManger, username, nil, nil)
}

func CreateInitialAdminUserWithKeys(ctx context.Context, authService auth.Service, metadataManager auth.MetadataManager, username string, accessKeyID *string, secretAccessKey *string) (*model.Credential, error) {
	adminUser := &model.SuperuserConfiguration{
		User: model.User{
			CreatedAt: time.Now(),
			Username:  username,
		},
	}
	if accessKeyID != nil && secretAccessKey != nil {
		adminUser.AccessKeyID = *accessKeyID
		adminUser.SecretAccessKey = *secretAccessKey
	}

	// create first admin user
	cred, err := AddAdminUser(ctx, authService, adminUser)
	if err != nil {
		return nil, err
	}

	if err = metadataManager.UpdateSetupTimestamp(ctx, time.Now()); err != nil {
		logging.FromContext(ctx).WithError(err).Error("Failed the update setup timestamp")
	}

	return cred, err
}
