package cmd

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/cmd/lakefs/application"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/logging"
)

// setupCmd initial lakeFS system setup - build database, load initial data and create first superuser
var setupCmd = &cobra.Command{
	Use:     "setup",
	Aliases: []string{"init"},
	Short:   "Setup a new LakeFS instance with initial credentials",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		ctx := cmd.Context()
		logger := logging.Default()
		lakeFsCmdContext := application.NewLakeFsCmdContext(ctx, cfg, logger)
		databaseService := application.NewDatabaseService(lakeFsCmdContext)
		defer databaseService.Close()
		err := databaseService.Migrate(ctx)
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}
		userCreator := func(ctx context.Context,
			authService *auth.DBAuthService,
			metadataManager *auth.DBMetadataManager,
			user *User) (*model.Credential, error) {
			return auth.CreateInitialAdminUserWithKeys(ctx,
				authService,
				metadataManager,
				user.userName, &user.accessKeyID, &user.secretAccessKey)
		}
		createUser(cmd, true, databaseService, cfg, ctx, userCreator)

	},
}

const internalErrorCode = 2

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(setupCmd)
	f := setupCmd.Flags()
	f.String("user-name", "", "an identifier for the user (e.g. \"jane.doe\")")
	f.String("access-key-id", "", "AWS-format access key ID to create for that user (for integration)")
	f.String("secret-access-key", "", "AWS-format secret access key to create for that user (for integration)")
	if err := f.MarkHidden("access-key-id"); err != nil {
		// (internal error)
		fmt.Fprint(os.Stderr, err)
		os.Exit(internalErrorCode)
	}
	if err := f.MarkHidden("secret-access-key"); err != nil {
		// (internal error)
		fmt.Fprint(os.Stderr, err)
		os.Exit(internalErrorCode)
	}
	_ = setupCmd.MarkFlagRequired("user-name")
}
