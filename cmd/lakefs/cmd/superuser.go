package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/stats"
)

// superuserCmd represents the init command
var superuserCmd = &cobra.Command{
	Use:   "superuser",
	Short: "Create additional user with admin credentials",
	Run: func(cmd *cobra.Command, args []string) {
		dbPool := db.BuildDatabaseConnection(cfg.GetDatabaseParams())
		defer func() { _ = dbPool.Close() }()

		userName, _ := cmd.Flags().GetString("user-name")

		authService := auth.NewDBAuthService(
			dbPool,
			crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
			cfg.GetAuthCacheConfig())
		authMetadataManager := auth.NewDBMetadataManager(config.Version, dbPool)
		metadataProvider := stats.BuildMetadataProvider(logging.Default(), cfg)
		metadata := stats.NewMetadata(logging.Default(), cfg, authMetadataManager, metadataProvider)
		credentials, err := auth.AddAdminUser(authService, &model.User{
			CreatedAt: time.Now(),
			Username:  userName,
		})
		if err != nil {
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(context.Background())
		stats := stats.NewBufferedCollector(metadata.InstallationID, cfg)
		go stats.Run(ctx)
		stats.CollectMetadata(metadata)
		stats.CollectEvent("global", "superuser")

		fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
			credentials.AccessKeyID, credentials.AccessSecretKey)

		cancelFn()
		<-stats.Done()
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(superuserCmd)
	superuserCmd.Flags().String("user-name", "", "an identifier for the user (e.g. \"jane.doe\")")
	_ = superuserCmd.MarkFlagRequired("user-name")
}
