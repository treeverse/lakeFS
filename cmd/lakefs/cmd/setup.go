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
	"github.com/treeverse/lakefs/stats"
)

// setupCmd represents the setup command
var setupCmd = &cobra.Command{
	Use:   "setup",
	Aliases: []string{"init"},
	Short: "Initialize a LakeFS instance, and setup an admin credential",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		migrator := db.NewDatabaseMigrator(cfg.GetDatabaseParams())
		err := migrator.Migrate(ctx)
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}

		dbPool := db.BuildDatabaseConnection(cfg.GetDatabaseParams())
		defer func() { _ = dbPool.Close() }()

		userName, _ := cmd.Flags().GetString("user-name")

		authService := auth.NewDBAuthService(
			dbPool,
			crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
			cfg.GetAuthCacheConfig())

		metaManager := auth.NewDBMetadataManager(config.Version, dbPool)
		metadata, err := metaManager.Write()
		if err != nil {
			fmt.Printf("failed to write initial setup metadata: %s\n", err)
			os.Exit(1)
		}

		credentials, err := auth.SetupAdminUser(authService, &model.User{
			CreatedAt: time.Now(),
			Username:  userName,
		})
		if err != nil {
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(context.Background())
		processID, bufferedCollectorArgs := cfg.GetStatsBufferedCollectorArgs()
		stats := stats.NewBufferedCollector(metadata[auth.InstallationIDKeyName], processID, bufferedCollectorArgs...)
		go stats.Run(ctx)
		stats.CollectMetadata(metadata)
		stats.CollectEvent("global", "setup")

		fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
			credentials.AccessKeyID, credentials.AccessSecretKey)

		cancelFn()
		<-stats.Done()
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(setupCmd)
	rootCmd.Flags().MarkDeprecated("init", "use 'lakefs setup' instead")
	setupCmd.Flags().String("user-name", "", "an identifier for the user (e.g. \"jane.doe\")")
	_ = setupCmd.MarkFlagRequired("user-name")
}
