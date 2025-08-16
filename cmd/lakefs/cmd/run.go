package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"text/template"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-co-op/gocron"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	apifactory "github.com/treeverse/lakefs/modules/api/factory"
	authfactory "github.com/treeverse/lakefs/modules/auth/factory"
	authenticationfactory "github.com/treeverse/lakefs/modules/authentication/factory"
	blockfactory "github.com/treeverse/lakefs/modules/block/factory"
	configfactory "github.com/treeverse/lakefs/modules/config/factory"
	gatewayfactory "github.com/treeverse/lakefs/modules/gateway/factory"
	licensefactory "github.com/treeverse/lakefs/modules/license/factory"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/gateway"
	"github.com/treeverse/lakefs/pkg/gateway/multipart"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	_ "github.com/treeverse/lakefs/pkg/kv/cosmosdb"
	_ "github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/local"
	"github.com/treeverse/lakefs/pkg/kv/mem"
	_ "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/upload"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	gracefulShutdownTimeout = 30 * time.Second

	mismatchedReposFlagName = "allow-mismatched-repos"
)

type Shutter interface {
	Shutdown(context.Context) error
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run lakeFS",
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.ContextUnavailable()
		cfg := LoadConfig()
		baseCfg := cfg.GetBaseConfig()
		viper.WatchConfig()
		viper.OnConfigChange(func(in fsnotify.Event) {
			// get current level before calling BuildConfig
			currentLevel := logging.Level()
			c, err := configfactory.BuildConfig("")
			if err != nil {
				logger.WithError(err).Error("Failed to reload configuration")
				return
			}
			baseCfg := c.GetBaseConfig()
			if baseCfg.Logging.Level != currentLevel {
				logger.WithField("level", baseCfg.Logging.Level).Info("Update logging level")
				logging.SetLevel(baseCfg.Logging.Level)
			}
		})

		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		logger.WithField("version", version.Version).Info("lakeFS run")

		kvParams, err := kvparams.NewConfig(&baseCfg.Database)
		if err != nil {
			logger.WithError(err).Fatal("Get KV params")
		}
		kvStore, err := kv.Open(ctx, enableKVParamsMetrics(kvParams))
		if err != nil {
			logger.WithError(err).Fatal("Failed to open KV store")
		}
		defer kvStore.Close()

		_, err = kv.ValidateSchemaVersion(ctx, kvStore)
		if err != nil && !errors.Is(err, kv.ErrNotFound) {
			logger.WithError(err).Fatal("Failure on schema validation")
		}

		licenseManager, err := licensefactory.NewLicenseManager(ctx, cfg)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create license manager")
		}
		err = licenseManager.ValidateLicense()
		if err != nil {
			logger.WithError(err).Fatal("License validation failed")
		}

		migrator := kv.NewDatabaseMigrator(kvParams)
		multipartTracker := multipart.NewTracker(kvStore)
		actionsStore := actions.NewActionsKVStore(kvStore)
		installationID := licenseManager.InstallationID()
		if installationID == "" {
			installationID = baseCfg.Installation.FixedID
		}
		authMetadataManager := auth.NewKVMetadataManager(version.Version, installationID, baseCfg.Database.Type, kvStore)
		idGen := &actions.DecreasingIDGenerator{}

		authService, err := authfactory.NewAuthService(ctx, cfg, logger, kvStore, authMetadataManager)
		if err != nil {
			logger.WithError(err).Fatal("failed to create authorization service")
		}

		authenticationService, err := authenticationfactory.NewAuthenticationService(ctx, cfg, logger)
		if err != nil {
			logger.WithError(err).Fatal("failed to create authentication service")
		}

		blockstoreType := baseCfg.Blockstore.Type
		if blockstoreType == "mem" {
			printLocalWarning(os.Stderr, fmt.Sprintf("blockstore type %s", blockstoreType))
			logger.WithField("adapter_type", blockstoreType).Warn("Block adapter NOT SUPPORTED for production use")
		}

		metadata := initStatsMetadata(ctx, logger, authMetadataManager, cfg.StorageConfig())
		bufferedCollector := stats.NewBufferedCollector(metadata.InstallationID, stats.Config(baseCfg.Stats),
			stats.WithLogger(logger.WithField("service", "stats_collector")))

		// init block store
		blockStore, err := blockfactory.BuildBlockAdapter(ctx, bufferedCollector, cfg)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create block adapter")
		}

		bufferedCollector.SetRuntimeCollector(blockStore.RuntimeStats)
		// send metadata
		bufferedCollector.CollectMetadata(metadata)

		c, err := catalog.New(ctx, catalog.Config{
			Config:       cfg,
			KVStore:      kvStore,
			PathProvider: upload.DefaultPathProvider,
		})
		if err != nil {
			logger.WithError(err).Fatal("failed to create catalog")
		}
		defer func() { _ = c.Close() }()

		// usage report setup - default usage reporter ids a no-op
		usageReporter := stats.DefaultUsageReporter
		if baseCfg.UsageReport.Enabled {
			ur := stats.NewUsageReporter(metadata.InstallationID, kvStore)
			ur.Start(ctx, baseCfg.UsageReport.FlushInterval, logger.WithField("service", "usage_report"))
			usageReporter = ur
		}

		deleteScheduler := gocron.NewScheduler(time.UTC)
		err = scheduleCleanupJobs(ctx, deleteScheduler, c)
		if err != nil {
			logger.WithError(err).Fatal("Failed to schedule cleanup jobs")
		}
		deleteScheduler.StartAsync()

		// initial setup - support only when a local database is configured.
		// local database lock will make sure that only one instance will run the setup.
		if (kvParams.Type == local.DriverName || kvParams.Type == mem.DriverName) &&
			baseCfg.Installation.UserName != "" && baseCfg.Installation.AccessKeyID.SecureValue() != "" && baseCfg.Installation.SecretAccessKey.SecureValue() != "" {
			setupCreds, err := setupLakeFS(ctx, cfg, authMetadataManager, authService, baseCfg.Installation.UserName,
				baseCfg.Installation.AccessKeyID.SecureValue(), baseCfg.Installation.SecretAccessKey.SecureValue(), false)
			if err != nil {
				logger.WithError(err).WithField("admin", baseCfg.Installation.UserName).Fatal("Failed to initial setup environment")
			}
			if setupCreds != nil {
				logger.WithField("admin", baseCfg.Installation.UserName).Info("Initial setup completed successfully")
			}
		}

		actionsService := actions.NewService(
			ctx,
			actionsStore,
			catalog.NewActionsSource(c),
			catalog.NewActionsOutputWriter(c.BlockAdapter),
			idGen,
			bufferedCollector,
			actions.Config(baseCfg.Actions),
			baseCfg.ListenAddress,
		)

		// wire actions into entry catalog
		defer actionsService.Stop()
		c.SetHooksHandler(actionsService)

		middlewareAuthenticator, err := authenticationfactory.BuildAuthenticatorChain(cfg, logger, authService)
		if err != nil {
			logger.WithError(err).Fatal("failed to create authentication chain")
		}

		auditChecker := version.NewDefaultAuditChecker(baseCfg.Security.AuditCheckURL, metadata.InstallationID, version.NewDefaultVersionSource(baseCfg.Security.CheckLatestVersionCache))
		defer auditChecker.Close()
		if !version.IsVersionUnreleased() {
			auditChecker.StartPeriodicCheck(ctx, baseCfg.Security.AuditCheckInterval, logger)
		}

		allowForeign, err := cmd.Flags().GetBool(mismatchedReposFlagName)
		if err != nil {
			logger.WithError(err).Fatal(mismatchedReposFlagName)
		}
		if !allowForeign {
			checkRepos(ctx, logger, cfg, authMetadataManager, blockStore, c)
		}

		// update health info with installation ID
		httputil.SetHealthHandlerInfo(metadata.InstallationID)

		// start API server
		apiHandler := api.Serve(
			cfg,
			c,
			middlewareAuthenticator,
			authService,
			authenticationService,
			blockStore,
			authMetadataManager,
			migrator,
			bufferedCollector,
			actionsService,
			auditChecker,
			logger.WithField("service", "api_gateway"),
			baseCfg.Gateways.S3.DomainNames,
			cfg.UIConfig().GetSnippets(),
			upload.DefaultPathProvider,
			usageReporter,
			licenseManager,
		)

		// init gateway server
		var s3FallbackURL *url.URL
		if baseCfg.Gateways.S3.FallbackURL != "" {
			s3FallbackURL, err = url.Parse(baseCfg.Gateways.S3.FallbackURL)
			if err != nil {
				logger.WithError(err).Fatal("Failed to parse s3 fallback URL")
			}
		}

		// setup authenticator for s3 gateway to also support swagger auth
		authCfg := cfg.AuthConfig()
		oidcConfig := api.OIDCConfig(authCfg.OIDC)
		cookieAuthConfig := api.CookieAuthConfig(authCfg.CookieAuthVerification)
		apiAuthenticator, err := api.GenericAuthMiddleware(
			logger.WithField("service", "s3_gateway"),
			middlewareAuthenticator,
			authService,
			&oidcConfig,
			&cookieAuthConfig,
		)
		if err != nil {
			logger.WithError(err).Fatal("could not initialize authenticator for S3 gateway")
		}

		middlewareFactory, err := gatewayfactory.BuildMiddleware(ctx, cfg, logger)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create gateway middleware")
		}

		s3gatewayHandler := gateway.NewHandler(
			baseCfg.Gateways.S3.Region,
			c,
			multipartTracker,
			blockStore,
			authService,
			baseCfg.Gateways.S3.DomainNames,
			bufferedCollector,
			upload.DefaultPathProvider,
			s3FallbackURL,
			baseCfg.Logging.AuditLogLevel,
			baseCfg.Logging.TraceRequestHeaders,
			baseCfg.Gateways.S3.VerifyUnsupported,
			authService.IsAdvancedAuth(),
			middlewareFactory.Build(),
		)
		s3gatewayHandler = apiAuthenticator(s3gatewayHandler)

		bufferedCollector.Start(ctx)
		defer bufferedCollector.Close()

		bufferedCollector.CollectEvent(stats.Event{Class: "global", Name: "run"})

		logger.WithField("listen_address", baseCfg.ListenAddress).Info("starting HTTP server")
		server := &http.Server{
			Addr:              baseCfg.ListenAddress,
			ReadHeaderTimeout: time.Minute,
			Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				// If the request has the S3 GW domain (exact or subdomain) - or carries an AWS sig, serve S3GW
				if httputil.HostMatches(request, baseCfg.Gateways.S3.DomainNames) ||
					httputil.HostSubdomainOf(request, baseCfg.Gateways.S3.DomainNames) ||
					sig.IsAWSSignedRequest(request) {
					s3gatewayHandler.ServeHTTP(writer, request)
					return
				}

				// Otherwise, serve the API handler
				apiHandler.ServeHTTP(writer, request)
			}),
		}

		actionsService.SetEndpoint(server)

		// register additional API services
		err = apifactory.RegisterServices(ctx, apifactory.ServiceDependencies{
			Config:                cfg,
			Authenticator:         middlewareAuthenticator,
			AuthService:           authService,
			AuthenticationService: authenticationService,
			BlockAdapter:          blockStore,
			Collector:             bufferedCollector,
			Logger:                logger,
			LicenseManager:        licenseManager,
		}, apiHandler)
		if err != nil {
			logger.WithError(err).Fatal("Failed to register services on router")
		}

		go func() {
			var err error
			if baseCfg.TLS.Enabled {
				err = server.ListenAndServeTLS(baseCfg.TLS.CertFile, baseCfg.TLS.KeyFile)
			} else {
				err = server.ListenAndServe()
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to listen on %s: %v\n", baseCfg.ListenAddress, err)
				os.Exit(1)
			}
		}()

		isQuickstart, err := cmd.Flags().GetBool(config.QuickstartConfiguration)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to get command flag %s: %v\n", config.QuickstartConfiguration, err)
			os.Exit(1)
		}

		data := bannerData{
			SetupMessage: localBanner,
			Version:      version.Version,
		}
		if isQuickstart {
			data.SetupMessage = quickStartBanner
		}

		var buf bytes.Buffer
		err = bannerTemplate.Execute(&buf, data)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed formatting banner: %v\n", err)
			os.Exit(1)
		}
		printWelcome(os.Stderr, buf.String())
		gracefulShutdown(ctx, server)
	},
}

// checkRepos iterating on all repos and validates that their settings are correct.
func checkRepos(ctx context.Context, logger logging.Logger, config config.Config, authMetadataManager auth.MetadataManager, blockStore block.Adapter, c *catalog.Catalog) {
	initialized, err := authMetadataManager.IsInitialized(ctx)
	if err != nil {
		logger.WithError(err).Fatal("Failed to check if lakeFS is initialized")
	}
	if !initialized {
		logger.Debug("lakeFS isn't initialized, skipping mismatched adapter checks")
	} else {
		logger.
			WithField("adapter_type", blockStore.BlockstoreType()).
			Debug("lakeFS is initialized, checking repositories for mismatched adapter")
		hasMore := true
		next := ""

		for hasMore {
			var err error
			var repos []*catalog.Repository
			repos, hasMore, err = c.ListRepositories(ctx, -1, "", "", next)
			if err != nil {
				logger.WithError(err).Fatal("Checking existing repositories failed")
			}

			for _, repo := range repos {
				adapterConfig := config.StorageConfig().GetStorageByID(repo.StorageID)
				if adapterConfig == nil {
					logger.Fatalf("No storage configuration found for repository '%s', StorageID='%s'", repo.Name, repo.StorageID)
					continue
				}
				adapterStorageType := adapterConfig.BlockstoreType()
				nsURL, err := url.Parse(repo.StorageNamespace)
				if err != nil {
					logger.WithError(err).Fatalf("Failed to parse repository %s namespace '%s'", repo.Name, repo.StorageNamespace)
				}
				repoStorageType, err := block.GetStorageType(nsURL)
				if err != nil {
					logger.WithError(err).Fatalf("Failed to parse to parse storage type '%s'", nsURL)
				}

				checkForeignRepo(repoStorageType, logger, adapterStorageType, repo.Name)
				next = repo.Name
			}
		}
	}
}

func scheduleCleanupJobs(ctx context.Context, s *gocron.Scheduler, c *catalog.Catalog) error {
	const deleteExpiredTaskInterval = 24 * time.Hour

	jobData := []struct {
		name     string
		interval time.Duration
		fn       func(context.Context)
	}{
		{
			name:     "delete expired imports",
			interval: ref.ImportExpiryTime,
			fn:       c.DeleteExpiredImports,
		},
		{
			name:     "delete expired tasks",
			interval: deleteExpiredTaskInterval,
			fn:       c.DeleteExpiredTasks,
		},
	}

	for _, jd := range jobData {
		job, err := s.Every(jd.interval).Do(jd.fn, ctx)
		if err != nil {
			return fmt.Errorf("schedule %s failed: %w", jd.name, err)
		}
		job.SingletonMode()
	}
	return nil
}

// checkForeignRepo checks whether a repo storage namespace matches the block adapter.
// A foreign repo is a repository which namespace doesn't match the current block adapter.
// A foreign repo might exist if the lakeFS instance configuration changed after a repository was
// already created.
// The behavior of lakeFS for foreign repos is undefined and should be blocked.
func checkForeignRepo(repoStorageType block.StorageType, logger logging.Logger, adapterStorageType, repoName string) {
	if adapterStorageType != repoStorageType.BlockstoreType() {
		logger.Fatalf("Mismatched adapter detected. lakeFS started with adapter of type '%s', but repository '%s' is of type '%s'",
			adapterStorageType, repoName, repoStorageType.BlockstoreType())
	}
}

var bannerTemplate = template.Must(template.New("banner").Parse(runBannerTmpl))

const runBannerTmpl = `
lakeFS {{ .Version }} - Up and running (^C to shutdown)...


     ██╗      █████╗ ██╗  ██╗███████╗███████╗███████╗
     ██║     ██╔══██╗██║ ██╔╝██╔════╝██╔════╝██╔════╝
     ██║     ███████║█████╔╝ █████╗  █████╗  ███████╗
     ██║     ██╔══██║██╔═██╗ ██╔══╝  ██╔══╝  ╚════██║
     ███████╗██║  ██║██║  ██╗███████╗██║     ███████║
     ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝     ╚══════╝
{{ .SetupMessage }}
│
│ For more information on how to use lakeFS,
│     check out the docs at https://docs.lakefs.io/quickstart/
│

│
│ For support or any other question,                            >(.＿.)<
│     join our Slack channel https://docs.lakefs.io/slack         (  )_
│

`

const localBanner = `
│
│ If you're running lakeFS locally for the first time,
│     complete the setup process at http://127.0.0.1:8000/setup
│`

var quickStartBanner = fmt.Sprintf(`
│
│ lakeFS running in quickstart mode.
│     Login at http://127.0.0.1:8000/
│
│     Access Key ID    : %s
│     Secret Access Key: %s
│
`, config.DefaultQuickstartKeyID, config.DefaultQuickstartSecretKey)

type bannerData struct {
	SetupMessage string
	Version      string
}

func printWelcome(w io.Writer, banner string) {
	_, _ = fmt.Fprint(w, banner)
	_, _ = fmt.Fprintf(w, "Version %s\n\n", version.Version)
}

const localWarningBanner = `
WARNING!

Using %s. This is suitable only for testing! It is NOT SUPPORTED for production.
`

func printLocalWarning(w io.Writer, msg string) {
	_, _ = fmt.Fprintf(w, localWarningBanner, msg)
}

func gracefulShutdown(ctx context.Context, services ...Shutter) {
	<-ctx.Done()

	_, _ = fmt.Fprintf(os.Stderr, "Shutting down...\n")
	ctx, cancel := context.WithTimeout(ctx, gracefulShutdownTimeout)
	defer cancel()
	for i, service := range services {
		if err := service.Shutdown(ctx); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed shutting down service [%d]: %s\n", i, err)
		}
	}
}

// enableKVParamsMetrics returns a copy of params.KV with postgres metrics enabled.
func enableKVParamsMetrics(p kvparams.Config) kvparams.Config {
	if p.Postgres == nil || p.Postgres.Metrics {
		return p
	}
	// make a copy of postgres settings and set metrics on
	pg := *p.Postgres
	pg.Metrics = true
	p.Postgres = &pg
	return p
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().BoolP(mismatchedReposFlagName, "m", false, "Allow repositories from other object store types")
	if err := runCmd.Flags().MarkHidden(mismatchedReposFlagName); err != nil {
		// (internal error)
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(internalErrorCode)
	}
}
