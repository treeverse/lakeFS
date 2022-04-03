package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakefs/application"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/version"
)

type User struct {
	userName        string
	accessKeyID     string
	secretAccessKey string
}

type OperationType int

const (
	InitialSetup OperationType = iota
	AddSuperUser
)

func getUserPropertyFromCmdArgs(cmd *cobra.Command, propertyName string) (*string, error) {
	propertyValue, err := cmd.Flags().GetString(propertyName)
	if err != nil {
		return nil, fmt.Errorf("invalid command line arg (%s): %w", propertyName, err)
	}
	return &propertyValue, nil
}
func parseUserFromCmdArgs(cmd *cobra.Command) (*User, error) {
	userName, err := getUserPropertyFromCmdArgs(cmd, "user-name")
	if err != nil {
		return nil, err
	}
	accessKeyID, err := getUserPropertyFromCmdArgs(cmd, "access-key-id")
	if err != nil {
		return nil, err
	}
	secretAccessKey, err := getUserPropertyFromCmdArgs(cmd, "secret-access-key")
	if err != nil {
		return nil, err
	}
	return &User{
		*userName,
		*accessKeyID,
		*secretAccessKey,
	}, nil
}

func createUser(cmd *cobra.Command,
	operationType OperationType,
	databaseService *application.DatabaseService,
	cfg *config.Config,
	logger logging.Logger,
	ctx context.Context) {
	user, err := parseUserFromCmdArgs(cmd)
	if err != nil {
		logger.WithError(err).Fatal("\n")
	}
	authService := databaseService.NewDBAuthService(cfg)
	metadataManager := databaseService.NewDBMetadataManager(cfg, version.Version)
	cloudMetadataProvider := stats.BuildMetadataProvider(logging.Default(), cfg)
	metadata := stats.NewMetadata(ctx, logging.Default(), cfg.GetBlockstoreType(), metadataManager, cloudMetadataProvider)
	var credentials *model.Credential

	if operationType == InitialSetup {
		ensureIsFirstInitializationOrFail(ctx, logger, metadataManager)
		credentials, err = auth.CreateInitialAdminUserWithKeys(ctx,
			authService,
			metadataManager,
			user.userName, &user.accessKeyID, &user.secretAccessKey)
	} else if operationType == AddSuperUser {
		credentials, err = auth.AddAdminUser(ctx, authService, &model.SuperuserConfiguration{
			User: model.User{
				CreatedAt: time.Now(),
				Username:  user.userName,
			},
			AccessKeyID:     user.accessKeyID,
			SecretAccessKey: user.secretAccessKey,
		})
	}
	if err != nil {
		logger.WithError(err).Fatal("Failed to setup admin user")
	}
	startCollectingAndLogCredentials(ctx, credentials, metadata, cfg, operationType)
}

func ensureIsFirstInitializationOrFail(ctx context.Context, logger logging.Logger, metadataManager auth.MetadataManager) {
	initialized, err := metadataManager.IsInitialized(ctx)
	if err != nil {
		logger.WithError(err).Fatal("Setup failed")
	}
	if initialized {
		logger.Fatal("Setup is already complete")
	}
}

func startCollectingAndLogCredentials(ctx context.Context, credentials *model.Credential, metadata *stats.Metadata, cfg *config.Config, operationType OperationType) {
	ctx, cancelFn := context.WithCancel(ctx)
	stats := stats.NewBufferedCollector(metadata.InstallationID, cfg)
	stats.Run(ctx)
	defer stats.Close()
	stats.CollectMetadata(metadata)
	var eventName string
	if operationType == InitialSetup {
		eventName = "init"
	} else {
		eventName = "superuser"
	}
	stats.CollectEvent("global", eventName)
	fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
		credentials.AccessKeyID, credentials.SecretAccessKey)
	cancelFn()
}
