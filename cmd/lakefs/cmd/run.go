package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/index"
)

const (
	gracefulShutdownTimeout = 30 * time.Second

	defaultInstallationID = "anon@example.com"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a LakeFS instance",
	Run: func(cmd *cobra.Command, args []string) {
		logging.Default().WithField("version", config.Version).Info("lakeFS run")

		mdb := cfg.ConnectMetadataDatabase()
		adb := cfg.ConnectAuthDatabase()
		defer func() {
			_ = adb.Close()
			_ = mdb.Close()
		}()
		migrator := db.NewDatabaseMigrator()
		for name, key := range config.SchemaDBKeys {
			migrator.AddDB(name, cfg.GetDatabaseURI(key))
		}

		// init index
		meta := index.NewDBIndex(mdb)

		// init block store
		blockStore := cfg.BuildBlockAdapter()

		// init authentication
		authService := auth.NewDBAuthService(adb, crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()))

		ctx, cancelFn := context.WithCancel(context.Background())
		stats := cfg.BuildStats(getInstallationID(authService))

		// start API server
		apiServer := api.NewServer(meta, blockStore, authService, stats, migrator)

		done := make(chan bool, 1)
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt)

		go func() {
			if err := apiServer.Listen(cfg.GetAPIListenAddress()); err != nil && err != http.ErrServerClosed {
				fmt.Printf("API server failed to listen on %s: %v\n", cfg.GetAPIListenAddress(), err)
				os.Exit(1)
			}
		}()

		// init gateway server
		gatewayServer := gateway.NewServer(
			cfg.GetS3GatewayRegion(),
			meta,
			blockStore,
			authService,
			cfg.GetS3GatewayListenAddress(),
			cfg.GetS3GatewayDomainName(),
			stats,
		)

		go stats.Run(ctx)
		stats.Collect("global", "run")

		go func() {
			if err := gatewayServer.Listen(); err != nil && err != http.ErrServerClosed {
				fmt.Printf("Gateway server failed to listen on %s: %v\n", cfg.GetS3GatewayListenAddress(), err)
				os.Exit(1)
			}
		}()

		go gracefulShutdown(apiServer, gatewayServer, quit, done)

		logging.Default().WithField("version", config.Version).Info("Up and running (^C to shutdown)...")

		<-done
		cancelFn()
		<-stats.Done()
	},
}

func getInstallationID(authService auth.Service) string {
	user, err := authService.GetFirstUser()
	if err != nil {
		return defaultInstallationID
	}
	return user.DisplayName
}

func gracefulShutdown(apiServer *api.Server, gatewayServer *gateway.Server, quit <-chan os.Signal, done chan<- bool) {
	<-quit
	logging.Default().Warn("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
	defer cancel()

	if err := apiServer.Shutdown(ctx); err != nil {
		fmt.Printf("Cloud not shutdown the API server: %v\n", err)
		os.Exit(1)
	}

	if err := gatewayServer.Shutdown(ctx); err != nil {
		fmt.Printf("Cloud not shutdown the gateway server: %v\n", err)
		os.Exit(1)
	}

	close(done)
}

func init() {
	rootCmd.AddCommand(runCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// runCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// runCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
