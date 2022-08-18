package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/version"
)

// setupCmd initial lakeFS system setup - build database, load initial data and create first superuser
var setupCmd = &cobra.Command{
	Use:     "setup",
	Aliases: []string{"init"},
	Short:   "Setup a new lakeFS instance with initial credentials",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()

		if cfg.IsAuthTypeAPI() {
			fmt.Printf("Can't setup lakeFS while using external auth API - auth.api.endpoint is configured.\n")
			os.Exit(1)
		}

		ctx := cmd.Context()
		dbParams := cfg.GetDatabaseParams()

		if len(dbParams.Type) > 0 && len(dbParams.ConnectionString) > 0 { // Conflicting configuration
			fmt.Printf("Conflicting database parameters, connection_string should be defined for the specific driver. Do you need to go through migration?\n")
			os.Exit(1)
		}
		var (
			dbPool   db.Database
			kvParams params.KV
			migrator db.Migrator
			err      error
		)
		if dbParams.KVEnabled {
			kvParams = cfg.GetKVParams()
			migrator = kv.NewDatabaseMigrator(kvParams)
		} else {
			dbPool = db.BuildDatabaseConnection(ctx, dbParams)
			defer dbPool.Close()

			migrator = db.NewDatabaseMigrator(dbParams)
		}

		err = migrator.Migrate(ctx)
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
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

		var (
			authService     auth.Service
			metadataManager auth.MetadataManager
		)
		authLogger := logging.Default().WithField("service", "auth_service")
		if dbParams.KVEnabled {
			kvStore, err := kv.Open(ctx, kvParams)
			if err != nil {
				fmt.Printf("Failed to connect to DB: %s", err)
				os.Exit(1)
			}
			defer kvStore.Close()
			storeMessage := &kv.StoreMessage{Store: kvStore}
			authService = auth.NewKVAuthService(storeMessage, crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()), nil, cfg.GetAuthCacheConfig(), authLogger)
			metadataManager = auth.NewKVMetadataManager(version.Version, cfg.GetFixedInstallationID(), cfg.GetDatabaseParams().Type, kvStore)
		} else {
			authService = auth.NewDBAuthService(dbPool, crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()), nil, cfg.GetAuthCacheConfig(), authLogger)
			metadataManager = auth.NewDBMetadataManager(version.Version, cfg.GetFixedInstallationID(), dbPool)
		}

		cloudMetadataProvider := stats.BuildMetadataProvider(logging.Default(), cfg)
		metadata := stats.NewMetadata(ctx, logging.Default(), cfg.GetBlockstoreType(), metadataManager, cloudMetadataProvider)

		initialized, err := metadataManager.IsInitialized(ctx)
		if err != nil {
			fmt.Printf("Setup failed: %s\n", err)
			os.Exit(1)
		}
		if initialized {
			fmt.Printf("Setup is already complete.\n")
			os.Exit(1)
		}

		credentials, err := auth.CreateInitialAdminUserWithKeys(ctx, authService, metadataManager, userName, &accessKeyID, &secretAccessKey)
		if err != nil {
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(ctx)
		stats := stats.NewBufferedCollector(metadata.InstallationID, cfg)
		stats.Run(ctx)
		defer stats.Close()

		stats.CollectMetadata(metadata)
		stats.CollectEvent("global", "init")

		fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
			credentials.AccessKeyID, credentials.SecretAccessKey)

		cancelFn()
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
