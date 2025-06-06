package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	authfactory "github.com/treeverse/lakefs/modules/auth/factory"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/version"
)

// superuserCmd represents the init command
var superuserCmd = &cobra.Command{
	Use:   "superuser",
	Short: "Create additional user with admin credentials",
	Long: `Create additional user with admin credentials.
This command can be used to import an admin user when moving from lakeFS version
with previously configured users to a lakeFS with basic auth version.
To do that provide the user name as well as the access key ID to import.
If the wrong user or credentials were chosen it is possible to delete the user and perform the action again.
`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := LoadConfig()
		authConfig := cfg.AuthConfig()
		baseConfig := cfg.GetBaseConfig()
		if authConfig.UIConfig.RBAC == config.AuthRBACExternal {
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

		logger := logging.ContextUnavailable()
		ctx := cmd.Context()
		kvParams, err := kvparams.NewConfig(&baseConfig.Database)
		if err != nil {
			fmt.Printf("KV params: %s\n", err)
			os.Exit(1)
		}
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			fmt.Printf("Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		addToAdmins := !authConfig.IsAuthBasic()
		authMetadataManager := auth.NewKVMetadataManager(version.Version, baseConfig.Installation.FixedID, baseConfig.Database.Type, kvStore)
		metadata := initStatsMetadata(ctx, logger, authMetadataManager, baseConfig.StorageConfig())
		authService, err := authfactory.NewAuthService(ctx, cfg, logger, kvStore, authMetadataManager)
		if err != nil {
			fmt.Printf("Failed to initialize auth service: %s\n", err)
			os.Exit(1)
		}

		credentials, err := setup.AddAdminUser(ctx, authService, &model.SuperuserConfiguration{
			User: model.User{
				CreatedAt: time.Now(),
				Username:  userName,
			},
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
		}, addToAdmins)
		if err != nil {
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(ctx)
		collector := stats.NewBufferedCollector(metadata.InstallationID, stats.Config(baseConfig.Stats),
			stats.WithLogger(logger.WithField("service", "stats_collector")))
		collector.Start(ctx)
		defer collector.Close()

		collector.CollectMetadata(metadata)
		collector.CollectEvent(stats.Event{Class: "global", Name: "superuser"})

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
