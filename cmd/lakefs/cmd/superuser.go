package cmd

import (
	"context"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakefs/application"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/logging"
)

// superuserCmd represents the init command
var superuserCmd = &cobra.Command{
	Use:   "superuser",
	Short: "Create additional user with admin credentials",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		ctx := cmd.Context()
		dbPool := db.BuildDatabaseConnection(ctx, cfg.GetDatabaseParams())
		defer dbPool.Close()
		logger := logging.Default()
		lakeFsCmdContext := application.NewLakeFsCmdContext(ctx, cfg, logger)
		databaseService := application.NewDatabaseService(lakeFsCmdContext)
		defer databaseService.Close()
		userCreator := func(ctx context.Context,
			authService *auth.DBAuthService,
			metadataManager *auth.DBMetadataManager,
			user *User) (*model.Credential, error) {
			return auth.AddAdminUser(ctx, authService, &model.SuperuserConfiguration{
				User: model.User{
					CreatedAt: time.Now(),
					Username:  user.userName,
				},
				AccessKeyID:     user.accessKeyID,
				SecretAccessKey: user.secretAccessKey,
			})
		}
		createUser(cmd, false, databaseService, cfg, ctx, userCreator)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(superuserCmd)
	f := superuserCmd.Flags()
	f.String("user-name", "", "an identifier for the user (e.g. \"jane.doe\")")
	f.String("access-key-id", "", "create this access key ID for the user (for ease of integration)")
	f.String("secret-access-key", "", "use this access key secret (potentially insecure, use carefully for ease of integration)")

	_ = superuserCmd.MarkFlagRequired("user-name")
}
