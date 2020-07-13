package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/auth/model"
)

// initCmd represents the init command
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a LakeFS instance, and setup an admin credential",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		migrator := db.NewDatabaseMigrator(cfg.GetDatabaseURI())
		err := migrator.Migrate(ctx)
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}

		dbPool := cfg.BuildDatabaseConnection()
		defer func() { _ = dbPool.Close() }()

		userName, _ := cmd.Flags().GetString("user-name")

		authService := auth.NewDBAuthService(
			dbPool,
			crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
			cfg.GetAuthCacheConfig())

		installationID, metadata, err := auth.WriteInitialMetadata(config.Version, authService)

		if err != nil {
			fmt.Printf("failed to write initial setup metadata: %s\n", err)
			os.Exit(1)
		}

		credentials, err := auth.SetupAdminUser(authService, &model.User{
			CreatedAt:   time.Now(),
			DisplayName: userName,
		})
		if err != nil {
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(context.Background())
		stats := cfg.BuildStats(installationID)
		go stats.Run(ctx)
		stats.CollectMetadata(metadata)
		stats.CollectEvent("global", "init")

		fmt.Printf("credentials:\naccess key id: %s\naccess secret key: %s\n", credentials.AccessKeyId, credentials.AccessSecretKey)

		cancelFn()
		<-stats.Done()
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
	initCmd.Flags().String("user-name", "", "display name for the user (e.g. \"jane.doe\")")
	_ = initCmd.MarkFlagRequired("user-name")
}
