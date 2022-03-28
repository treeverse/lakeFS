package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

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

var errInvalidUserPropertyCommandLineArg = errors.New("invalid user property command line arg")

type UserCreator = func(context.Context, *auth.DBAuthService, *auth.DBMetadataManager, *User) (*model.Credential, error)

func getUserPropertyFromCmdArgs(cmd *cobra.Command, propertyName string) (*string, error) {
	propertyValue, err := cmd.Flags().GetString(propertyName)
	if err != nil {
		return nil, fmt.Errorf("%w %s: %s", errInvalidUserPropertyCommandLineArg, propertyName, err)
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

func createUser(cmd *cobra.Command, checkFirstSuccesfulInitialization bool,
	databaseService *application.DatabaseService,
	cfg *config.Config,
	ctx context.Context, userCreator UserCreator) {
	user, err := parseUserFromCmdArgs(cmd)
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}

	authService := databaseService.NewDBAuthService(cfg)
	metadataManager := databaseService.NewDBMetadataManager(cfg, version.Version)

	cloudMetadataProvider := stats.BuildMetadataProvider(logging.Default(), cfg)
	metadata := stats.NewMetadata(ctx, logging.Default(), cfg.GetBlockstoreType(), metadataManager, cloudMetadataProvider)
	if checkFirstSuccesfulInitialization {
		initialized, err := metadataManager.IsInitialized(ctx)
		if err != nil {
			fmt.Printf("Setup failed: %s\n", err)
			os.Exit(1)
		}
		if initialized {
			fmt.Printf("Setup is already complete.\n")
			os.Exit(1)
		}
	}

	credentials, err := userCreator(ctx, authService, metadataManager, user)
	if err != nil {
		fmt.Printf("Failed to setup admin user: %s\n", err)
		os.Exit(1)
	}
	startCollectingAndLogCredentials(ctx, credentials, metadata, cfg)
}

func startCollectingAndLogCredentials(ctx context.Context, credentials *model.Credential, metadata *stats.Metadata, cfg *config.Config) {
	ctx, cancelFn := context.WithCancel(ctx)
	stats := stats.NewBufferedCollector(metadata.InstallationID, cfg)
	go stats.Run(ctx)
	stats.CollectMetadata(metadata)
	stats.CollectEvent("global", "init")

	fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
		credentials.AccessKeyID, credentials.SecretAccessKey)

	cancelFn()
	<-stats.Done()
}
