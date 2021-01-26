package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/golang-migrate/migrate/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/block/factory"
	catalogfactory "github.com/treeverse/lakefs/catalog/factory"
	catalogmigrate "github.com/treeverse/lakefs/catalog/migrate"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/dedup"
	"github.com/treeverse/lakefs/export"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/gateway/multiparts"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/parade"
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

		if err := db.ValidateSchemaUpToDate(dbParams); errors.Is(err, db.ErrSchemaNotCompatible) {
			logger.WithError(err).Fatal("Migration version mismatch, for more information see https://docs.lakefs.io/deploying/upgrade.html")
		} else if errors.Is(err, migrate.ErrNilVersion) {
			logger.Debug("No migration, setup required")
		} else if err != nil {
			logger.WithError(err).Warn("Failed on schema validation")
		}
		dbPool := db.BuildDatabaseConnection(dbParams)
		defer dbPool.Close()

		registerPrometheusCollector(dbPool)
		retention := retention.NewService(dbPool)
		migrator := db.NewDatabaseMigrator(dbParams)

		cataloger, err := catalogfactory.BuildCataloger(dbPool, cfg)
		if err != nil {
			logger.WithError(err).Fatal("failed to create cataloger")
		}
		multipartsTracker := multiparts.NewTracker(dbPool)

		// init block store
		blockStore, err := factory.BuildBlockAdapter(cfg)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create block adapter")
		}

		//  Migrate old MVCC - if cataloger type is not set,
		//  warn the user in case migrate didn't run and there are MVCC repositories
		if cfg.GetCatalogerType() == "" {
			migrationRequired := catalogmigrate.CheckMigrationRequired(dbPool)
			if migrationRequired {
				logger.Fatal(migrateRequiredMsg)
			}
		}

		// init authentication
		authService := auth.NewDBAuthService(
			dbPool,
			crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
			cfg.GetAuthCacheConfig())
		authMetadataManager := auth.NewDBMetadataManager(config.Version, dbPool)
		cloudMetadataProvider := stats.BuildMetadataProvider(logger, cfg)
		metadata := stats.NewMetadata(logger, cfg, authMetadataManager, cloudMetadataProvider)
		bufferedCollector := stats.NewBufferedCollector(metadata.InstallationID, cfg)

		// send metadata
		bufferedCollector.CollectMetadata(metadata)

		// update health info with installation ID
		httputil.SetHealthHandlerInfo(metadata.InstallationID)

		dedupCleaner := dedup.NewCleaner(blockStore, cataloger.DedupReportChannel())

		// parade
		paradeDB := parade.NewParadeDB(dbPool.Pool())
		// export handler
		exportHandler := export.NewHandler(blockStore, cataloger, paradeDB)
		exportActionManager := parade.NewActionManager(exportHandler, paradeDB, nil)
		defer func() {
			// order is important - close cataloger channel before dedup
			_ = cataloger.Close()
			_ = dedupCleaner.Close()
			exportActionManager.Close()
		}()

		// start API server
		done := make(chan bool, 1)
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

		apiHandler := api.NewHandler(
			cataloger,
			blockStore,
			authService,
			authMetadataManager,
			bufferedCollector,
			retention,
			migrator,
			paradeDB,
			dedupCleaner,
			logger.WithField("service", "api_gateway"),
		)

		// init gateway server
		s3Fallback := cfg.GetS3GatewayFallbackURL()
		var s3FallbackURL *url.URL
		if s3Fallback != "" {
			s3FallbackURL, err = url.Parse(s3Fallback)
			if err != nil {
				logger.WithError(err).Fatal("Failed to parse s3 fallback URL")
			}
		}
		s3gatewayHandler := gateway.NewHandler(
			cfg.GetS3GatewayRegion(),
			cataloger,
			multipartsTracker,
			blockStore,
			authService,
			cfg.GetS3GatewayDomainName(),
			bufferedCollector,
			dedupCleaner,
			s3FallbackURL,
		)
		ctx, cancelFn := context.WithCancel(context.Background())
		go bufferedCollector.Run(ctx)

		bufferedCollector.CollectEvent("global", "run")

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
		<-bufferedCollector.Done()
	},
}

const migrateRequiredMsg = `Data migration is required.
Starting version 0.30.0, lakeFS handles your committed metadata in a new way,
which is more robust and has better performance.
To move your existing data, you will need to run the following upgrade command:

  $ lakefs migrate db

If you want to start over, discarding your existing data, you need to explicitly state this in your lakeFS configuration file.
To do so, add the following to your configuration:

cataloger:
  type: rocks
`

const runBanner = `

     ██╗      █████╗ ██╗  ██╗███████╗███████╗███████╗
     ██║     ██╔══██╗██║ ██╔╝██╔════╝██╔════╝██╔════╝
     ██║     ███████║█████╔╝ █████╗  █████╗  ███████╗
     ██║     ██╔══██║██╔═██╗ ██╔══╝  ██╔══╝  ╚════██║
     ███████╗██║  ██║██║  ██╗███████╗██║     ███████║
     ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝     ╚══════╝

│
│ If you're running lakeFS locally for the first time,
│     complete the setup process at http://127.0.0.1:8000/setup
│

│
│ For more information on how to use lakeFS,
│     check out the docs at https://docs.lakefs.io/quickstart/repository
│

`

func printWelcome(w io.Writer) {
	_, _ = fmt.Fprint(w, runBanner)
	_, _ = fmt.Fprintf(w, "Version %s\nCataloger %s\n\n", config.Version, cfg.GetCatalogerType())
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

	printWelcome(os.Stderr)

	<-quit
	logger.Warn("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
	defer cancel()

	for i, server := range servers {
		if err := server.Shutdown(ctx); err != nil {
			fmt.Printf("Error while shutting down service (%d): %s\n", i, err)
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
