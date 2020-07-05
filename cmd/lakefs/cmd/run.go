package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index"

	"github.com/treeverse/lakefs/logging"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/gateway"
)

const (
	gracefulShutdownTimeout = 30 * time.Second

	defaultInstallationID = "unknown"

	serviceAPIServer = "api"
	serviceS3Gateway = "s3gateway"
)

type Shutter interface {
	Shutdown(context.Context) error
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run lakeFS",
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.Default()
		logger.WithField("version", config.Version).Infof("lakeFS run")
		services, err := cmd.Flags().GetStringArray("service")
		if err != nil {
			fmt.Printf("Failed to get value for 'services': %s\n", err)
			os.Exit(1)
		}

		// validate service names and turn on the right flags
		var runS3Gateway bool
		var runAPIService bool
		for _, service := range services {
			switch service {
			case serviceS3Gateway:
				runS3Gateway = true
			case serviceAPIServer:
				runAPIService = true
			default:
				fmt.Printf("Unknown service: %s\n", service)
				os.Exit(1)
			}
		}
		if !runS3Gateway && !runAPIService {
			fmt.Printf("No service to run\n")
			os.Exit(1)
		}

		dbConnString := cfg.GetDatabaseURI()
		dbPool := cfg.BuildDatabaseConnection()

		defer func() {
			_ = dbPool.Close()
		}()
		migrator := db.NewDatabaseMigrator(dbConnString)

		// init index
		meta := index.NewDBIndex(dbPool)

		// init block store
		blockStore := cfg.BuildBlockAdapter()

		// init authentication
		authService := auth.NewDBAuthService(dbPool, crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()))

		ctx, cancelFn := context.WithCancel(context.Background())
		stats := cfg.BuildStats(getInstallationID(authService))

		// start API server
		done := make(chan bool, 1)
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt)

		var apiServer *api.Server
		if runAPIService {
			apiServer = api.NewServer(
				meta, blockStore, authService, stats, migrator,
				logger.WithField("service", "api_gateway"))
			go func() {
				if err := apiServer.Listen(cfg.GetAPIListenAddress()); err != nil && err != http.ErrServerClosed {
					fmt.Printf("API server failed to listen on %s: %v\n", cfg.GetAPIListenAddress(), err)
					os.Exit(1)
				}
			}()
		}

		var gatewayServer *gateway.Server
		if runS3Gateway {
			// init gateway server
			gatewayServer = gateway.NewServer(
				cfg.GetS3GatewayRegion(),
				meta,
				blockStore,
				authService,
				cfg.GetS3GatewayListenAddress(),
				cfg.GetS3GatewayDomainName(),
				stats,
			)
		}

		go stats.Run(ctx)
		stats.CollectEvent("global", "run")

		metaUpdater := auth.NewMetadataRefresher(5*time.Minute, 24*time.Hour, authService, stats)
		metaUpdater.Start()

		if gatewayServer != nil {
			go func() {
				if err := gatewayServer.Listen(); err != nil && err != http.ErrServerClosed {
					fmt.Printf("Gateway server failed to listen on %s: %v\n", cfg.GetS3GatewayListenAddress(), err)
					os.Exit(1)
				}
			}()
		}
		go gracefulShutdown(quit, done, apiServer, gatewayServer, metaUpdater)
		<-done
		cancelFn()
		<-stats.Done()
	},
}

func getInstallationID(authService auth.Service) string {
	installID, err := authService.GetAccountMetadataKey("installation_id")
	if err != nil {
		return defaultInstallationID
	}
	return installID
}

func gracefulShutdown(quit <-chan os.Signal, done chan<- bool, servers ...Shutter) {
	logger := logging.Default()
	logger.WithField("version", config.Version).Info("Up and running (^C to shutdown)...")
	<-quit
	logger.Warn("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
	defer cancel()

	for i, server := range servers {
		if err := server.Shutdown(ctx); err != nil {
			fmt.Printf("Error while shutting down service (%d): %s\n", i, err)
			os.Exit(1)
		}
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
	runCmd.Flags().StringArrayP("service", "s", []string{serviceS3Gateway, serviceAPIServer}, "lakeFS services to run")
}
