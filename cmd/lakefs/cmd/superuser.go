package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/version"
)

// superuserCmd represents the init command
var superuserCmd = &cobra.Command{
	Use:   "superuser",
	Short: "Create additional user with admin credentials",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		if cfg.IsAuthTypeAPI() {
			fmt.Printf("Can't create additional admin while using external auth API - auth.api.endpoint is configured.\n")
			os.Exit(1)
		}

		userName, err := cmd.Flags().GetString("user-name")
		if err != nil {
			fmt.Printf("user-name: %s\n", err)
			os.Exit(1)
		}
		accessKeyID, err := cmd.Flags().GetString("access-key-id")
		if err != nil {
			fmt.Printf("access-key-id: %s\n", err)
			os.Exit(1)
		}
		secretAccessKey, err := cmd.Flags().GetString("secret-access-key")
		if err != nil {
			fmt.Printf("secret-access-key: %s\n", err)
			os.Exit(1)
		}

		logger := logging.Default()
		ctx := cmd.Context()
		dbParams := cfg.GetDatabaseParams()
		var (
			dbPool              db.Database
			authService         auth.Service
			authMetadataManager auth.MetadataManager
		)
		if dbParams.KVEnabled {
			kvParams := cfg.GetKVParams()
			kvStore, err := kv.Open(ctx, kvParams)
			if err != nil {
				fmt.Printf("failed to open KV store: %s\n", err)
				os.Exit(1)
			}
			storeMessage := &kv.StoreMessage{Store: kvStore}
			authService = auth.NewKVAuthService(storeMessage, crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()), nil, cfg.GetAuthCacheConfig(), logger.WithField("service", "auth_service"))
			authMetadataManager = auth.NewKVMetadataManager(version.Version, cfg.GetFixedInstallationID(), cfg.GetDatabaseParams().Type, kvStore)
		} else {
			dbPool = db.BuildDatabaseConnection(ctx, dbParams)
			defer dbPool.Close()
			authService = auth.NewDBAuthService(dbPool, crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()), nil, cfg.GetAuthCacheConfig(), logger.WithField("service", "auth_service"))
			authMetadataManager = auth.NewDBMetadataManager(version.Version, cfg.GetFixedInstallationID(), dbPool)
		}

		metadataProvider := stats.BuildMetadataProvider(logger, cfg)
		metadata := stats.NewMetadata(ctx, logger, cfg.GetBlockstoreType(), authMetadataManager, metadataProvider)
		credentials, err := auth.AddAdminUser(ctx, authService, &model.SuperuserConfiguration{
			User: model.User{
				CreatedAt: time.Now(),
				Username:  userName,
			},
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
		})
		if err != nil {
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(ctx)
		stats := stats.NewBufferedCollector(metadata.InstallationID, cfg)
		stats.Run(ctx)
		defer stats.Close()

		stats.CollectMetadata(metadata)
		stats.CollectEvent("global", "superuser")

		fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
			credentials.AccessKeyID, credentials.SecretAccessKey)

		cancelFn()
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
