package cmd

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/dedup"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/retention"
	"github.com/treeverse/lakefs/stats"
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
		dbParams := cfg.GetDatabaseParams()

		dbPool := db.BuildDatabaseConnection(dbParams)
		defer func() {
			_ = dbPool.Close()
		}()
		registerPrometheusCollector(dbPool)
		retention := retention.NewService(dbPool)
		migrator := db.NewDatabaseMigrator(dbParams)

		// init catalog
		cataloger := catalog.NewCataloger(dbPool)

		// init block store
		blockStore, err := factory.BuildBlockAdapter(cfg, dbPool)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create block adapter")
		}

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
		processID, bufferedCollectorArgs := cfg.GetStatsBufferedCollectorArgs()
		stats := stats.NewBufferedCollector(installationID, processID, bufferedCollectorArgs...)

		dedupCleaner := dedup.NewCleaner(blockStore, cataloger.DedupReportChannel())
		defer func() {
			// order is important - close cataloger channel before dedup
			_ = cataloger.Close()
			_ = dedupCleaner.Close()
		}()

		// start API server
		done := make(chan bool, 1)
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

		apiHandler := api.NewHandler(
			cataloger,
			blockStore,
			authService,
			meta,
			stats,
			retention,
			migrator,
			dedupCleaner,
			logger.WithField("service", "api_gateway"),
		)

		// init gateway server
		s3gatewayHandler := gateway.NewHandler(
			cfg.GetS3GatewayRegion(),
			cataloger,
			blockStore,
			authService,
			cfg.GetS3GatewayDomainName(),
			stats,
			dedupCleaner,
		)

		ctx, cancelFn := context.WithCancel(context.Background())
		go stats.Run(ctx)
		stats.CollectEvent("global", "run")

		// stagger a bit and update metadata
		go func() {
			// avoid a thundering herd in case we have many lakeFS instances starting together
			const maxSplay = 10 * time.Second
			randSource := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
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
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

func registerPrometheusCollector(db sqlstats.StatsGetter) {
	collector := sqlstats.NewStatsCollector("lakefs", db)
	err := prometheus.Register(collector)
	if err != nil {
		logging.Default().WithError(err).Error("failed to register db stats collector")
	}
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
	simulator.ShutdownRecorder()
	close(done)
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().StringArrayP("service", "s", []string{serviceS3Gateway, serviceAPIServer}, "lakeFS services to run")
}
