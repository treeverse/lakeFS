package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/logging"
)

const (
	gracefulShutdownTimeout = 30 * time.Second

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

		// validate service names and turn on the right flags
		dbConnString := cfg.GetDatabaseURI()
		dbPool := cfg.BuildDatabaseConnection()
		defer func() {
			_ = dbPool.Close()
		}()
		migrator := db.NewDatabaseMigrator(dbConnString)

		// init index
		index := index.NewDBIndex(dbPool)

		// init block store
		blockStore := cfg.BuildBlockAdapter()

		// init authentication
		authService := auth.NewDBAuthService(
			dbPool,
			crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
			cfg.GetAuthCacheConfig())

		meta := auth.NewDBMetadataManager(config.Version, dbPool)

		installationID, err := meta.InstallationID()
		if err != nil {
			installationID = "" // no installation ID is available
		}
		stats := cfg.BuildStats(installationID)

		// start API server
		done := make(chan bool, 1)
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt)

		apiHandler := api.NewHandler(
			index, blockStore, authService, meta, stats, migrator,
			logger.WithField("service", "api_gateway"))

		// init gateway server
		s3gatewayHandler := gateway.NewHandler(
			cfg.GetS3GatewayRegion(),
			index,
			blockStore,
			authService,
			cfg.GetS3GatewayDomainName(),
			stats)

		ctx, cancelFn := context.WithCancel(context.Background())
		go stats.Run(ctx)
		stats.CollectEvent("global", "run")

		// stagger a bit and update metadata
		go func() {
			// avoid a thundering herd in case we have many lakeFS instances starting together
			const maxSplay = 10 * time.Second
			randSource := rand.New(rand.NewSource(time.Now().UnixNano()))
			time.Sleep(time.Duration(randSource.Intn(int(maxSplay))))

			metadata, err := meta.Write()
			if err != nil {
				logger.WithError(err).Trace("failed to collect account metadata")
				return
			}
			stats.CollectMetadata(metadata)
		}()

		logging.Default().WithField("listen_address", cfg.GetListenAddress()).Info("starting HTTP server")
		server := &http.Server{
			Addr: cfg.GetListenAddress(),
			Handler: httputil.HostMux(
				httputil.HostHandler(apiHandler).Default(), // api as default handler
				httputil.HostHandler(s3gatewayHandler, // s3 gateway for its bare domain and sub-domains of that
					httputil.Exact(cfg.GetS3GatewayDomainName()),
					httputil.SubdomainsOf(cfg.GetS3GatewayDomainName())),
			),
		}

		go func() {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Printf("server failed to listen on %s: %v\n", cfg.GetListenAddress(), err)
				os.Exit(1)
			}
		}()

		go gracefulShutdown(quit, done, server)
		<-done
		cancelFn()
		<-stats.Done()
	},
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
