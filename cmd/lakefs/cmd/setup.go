package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/version"
)

// setupCmd initial lakeFS system setup - build the database, load initial data and create first superuser
var setupCmd = &cobra.Command{
	Use:     "setup",
	Aliases: []string{"init"},
	Short:   "Setup a new lakeFS instance with initial credentials",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()

		ctx := cmd.Context()
		kvParams, err := kvparams.NewConfig(cfg)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		migrator := kv.NewDatabaseMigrator(kvParams)

		err = migrator.Migrate(ctx)
		if err != nil {
			fmt.Printf("Failed to setup DB: %s\n", err)
			os.Exit(1)
		}

		if cfg.Auth.UIConfig.RBAC == config.AuthRBACExternal {
			// nothing to do - users are managed elsewhere
			return
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
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Printf("Failed to connect to DB: %s", err)
			os.Exit(1)
		}
		defer kvStore.Close()
		logger := logging.ContextUnavailable()
		authLogger := logger.WithField("service", "auth_service")
		authService = auth.NewAuthService(kvStore, crypt.NewSecretStore([]byte(cfg.Auth.Encrypt.SecretKey)), authparams.ServiceCache(cfg.Auth.Cache), authLogger)
		metadataManager = auth.NewKVMetadataManager(version.Version, cfg.Installation.FixedID, cfg.Database.Type, kvStore)

		cloudMetadataProvider := stats.BuildMetadataProvider(logger, cfg)
		metadata := stats.NewMetadata(ctx, logger, cfg.Blockstore.Type, metadataManager, cloudMetadataProvider)

		credentials, err := setupLakeFS(ctx, cfg, metadataManager, authService, userName, accessKeyID, secretAccessKey)
		if err != nil {
			fmt.Printf("Setup failed: %s\n", err)
			os.Exit(1)
		}
		if credentials == nil {
			fmt.Printf("Setup is already complete.\n")
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(ctx)
		collector := stats.NewBufferedCollector(metadata.InstallationID, stats.Config(cfg.Stats),
			stats.WithLogger(logger.WithField("service", "stats_collector")))
		collector.Start(ctx)
		defer collector.Close()

		collector.CollectMetadata(metadata)
		collector.CollectEvent(stats.Event{Class: "global", Name: "init"})

		fmt.Printf("credentials:\n  access_key_id: %s\n  secret_access_key: %s\n",
			credentials.AccessKeyID, credentials.SecretAccessKey)

		cancelFn()
	},
}

func setupLakeFS(ctx context.Context, cfg *config.Config, metadataManager auth.MetadataManager, authService auth.Service, userName string, accessKeyID string, secretAccessKey string) (*model.Credential, error) {
	initialized, err := metadataManager.IsInitialized(ctx)
	if err != nil || initialized {
		// return on error or if already initialized
		return nil, err
	}

	// mark comm prefs was not provided
	_, err = metadataManager.UpdateCommPrefs(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("update comm prefs: %w", err)
	}

	// populate initial data and create admin user
	credentials, err := setup.CreateInitialAdminUserWithKeys(ctx, authService, cfg, metadataManager, userName, &accessKeyID, &secretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("create initial admin user: %w", err)
	}
	return credentials, nil
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
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(internalErrorCode)
	}
	if err := f.MarkHidden("secret-access-key"); err != nil {
		// (internal error)
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(internalErrorCode)
	}
	_ = setupCmd.MarkFlagRequired("user-name")
}
