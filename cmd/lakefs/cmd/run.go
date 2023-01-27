package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/fsnotify/fsnotify"
	"github.com/go-co-op/gocron"
	"github.com/go-ldap/ldap/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/email"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/gateway"
	"github.com/treeverse/lakefs/pkg/gateway/multipart"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	_ "github.com/treeverse/lakefs/pkg/kv/dynamodb"
	_ "github.com/treeverse/lakefs/pkg/kv/local"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/kv/params"
	_ "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/templater"
	"github.com/treeverse/lakefs/pkg/upload"
	"github.com/treeverse/lakefs/pkg/version"
	"github.com/treeverse/lakefs/templates"
	"golang.org/x/oauth2"
)

const (
	gracefulShutdownTimeout = 30 * time.Second

	mismatchedReposFlagName = "allow-mismatched-repos"
)

type Shutter interface {
	Shutdown(context.Context) error
}

func newLDAPAuthenticator(cfg *config.LDAP, service auth.Service) *auth.LDAPAuthenticator {
	const (
		connectionTimeout = 15 * time.Second
		requestTimeout    = 7 * time.Second
	)
	group := cfg.DefaultUserGroup
	if group == "" {
		group = auth.ViewersGroup
	}
	return &auth.LDAPAuthenticator{
		AuthService:       service,
		BindDN:            cfg.BindDN,
		BindPassword:      cfg.BindPassword,
		DefaultUserGroup:  group,
		UsernameAttribute: cfg.UsernameAttribute,
		MakeLDAPConn: func(_ context.Context) (*ldap.Conn, error) {
			c, err := ldap.DialURL(
				cfg.ServerEndpoint,
				ldap.DialWithDialer(&net.Dialer{Timeout: connectionTimeout}),
			)
			if err != nil {
				return nil, fmt.Errorf("dial %s: %w", cfg.ServerEndpoint, err)
			}
			c.SetTimeout(requestTimeout)
			// TODO(ariels): Support StartTLS (& other TLS configuration).
			return c, nil
		},
		BaseSearchRequest: ldap.SearchRequest{
			BaseDN:     cfg.UserBaseDN,
			Scope:      ldap.ScopeWholeSubtree,
			Filter:     cfg.UserFilter,
			Attributes: []string{cfg.UsernameAttribute},
		},
	}
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run lakeFS",
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.Default()
		cfg := loadConfig()
		viper.WatchConfig()
		viper.OnConfigChange(func(in fsnotify.Event) {
			var c config.Config
			if err := config.Unmarshal(&c); err != nil {
				logger.WithError(err).Error("Failed to unmarshal config while reload")
				return
			}
			if c.Logging.Level != logging.Level() {
				logger.WithField("level", c.Logging.Level).Info("Update log level")
				logging.SetLevel(c.Logging.Level)
			}
		})

		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		logger.WithField("version", version.Version).Info("lakeFS run")

		kvParams, err := cfg.DatabaseParams()
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

		emailer, err := email.NewEmailer(email.Params(cfg.Email))
		if err != nil {
			logger.WithError(err).Fatal("Emailer has not been properly configured, check the values in sender field")
		}

		migrator := kv.NewDatabaseMigrator(kvParams)
		storeMessage := &kv.StoreMessage{Store: kvStore}
		multipartTracker := multipart.NewTracker(*storeMessage)
		actionsStore := actions.NewActionsKVStore(*storeMessage)
		authMetadataManager := auth.NewKVMetadataManager(version.Version, cfg.Installation.FixedID, cfg.Database.Type, kvStore)
		idGen := &actions.DecreasingIDGenerator{}

		// initialize auth service
		var authService auth.Service
		if cfg.IsAuthTypeAPI() {
			var apiEmailer *email.Emailer
			if !cfg.Auth.API.SupportsInvites {
				// invites not supported by API - delegate it to emailer
				apiEmailer = emailer
			}
			authService, err = auth.NewAPIAuthService(
				cfg.Auth.API.Endpoint,
				cfg.Auth.API.Token,
				crypt.NewSecretStore(cfg.AuthEncryptionSecret()),
				cfg.Auth.Cache, nil, apiEmailer)
			if err != nil {
				logger.WithError(err).Fatal("failed to create authentication service")
			}
		} else {
			authService = auth.NewAuthService(
				storeMessage,
				crypt.NewSecretStore(cfg.AuthEncryptionSecret()),
				emailer,
				cfg.Auth.Cache,
				logger.WithField("service", "auth_service"),
			)
		}

		cloudMetadataProvider := stats.BuildMetadataProvider(logger, cfg)
		blockstoreType := cfg.BlockstoreType()
		if blockstoreType == "local" || blockstoreType == "mem" {
			printLocalWarning(os.Stderr, fmt.Sprintf("blockstore type %s", blockstoreType))
			logger.WithField("adapter_type", blockstoreType).Warn("Block adapter NOT SUPPORTED for production use")
		}

		metadata := stats.NewMetadata(ctx, logger, blockstoreType, authMetadataManager, cloudMetadataProvider)
		bufferedCollector := stats.NewBufferedCollector(metadata.InstallationID, stats.Config(cfg.Stats),
			stats.WithLogger(logger.WithField("service", "stats_collector")))

		// init block store
		blockStore, err := factory.BuildBlockAdapter(ctx, bufferedCollector, cfg)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create block adapter")
		}
		bufferedCollector.SetRuntimeCollector(blockStore.RuntimeStats)
		// send metadata
		bufferedCollector.CollectMetadata(metadata)

		c, err := catalog.New(ctx, catalog.Config{
			Config:       cfg,
			KVStore:      storeMessage,
			PathProvider: upload.DefaultPathProvider,
		})
		if err != nil {
			logger.WithError(err).Fatal("failed to create catalog")
		}
		defer func() { _ = c.Close() }()

		deleteScheduler := getScheduler()
		err = scheduleCleanupJobs(ctx, deleteScheduler, c)
		if err != nil {
			logger.WithError(err).Fatal("Failed to schedule cleanup jobs")
		}
		deleteScheduler.StartAsync()

		templater := templater.NewService(templates.Content, cfg, authService)

		actionsService := actions.NewService(
			ctx,
			actionsStore,
			catalog.NewActionsSource(c),
			catalog.NewActionsOutputWriter(c.BlockAdapter),
			idGen,
			bufferedCollector,
			cfg.Actions.Enabled,
		)

		// wire actions into entry catalog
		defer actionsService.Stop()
		c.SetHooksHandler(actionsService)

		middlewareAuthenticator := auth.ChainAuthenticator{
			auth.NewBuiltinAuthenticator(authService),
		}

		if cfg.Auth.LDAP != nil {
			middlewareAuthenticator = append(middlewareAuthenticator, newLDAPAuthenticator(cfg.Auth.LDAP, authService))
		}
		controllerAuthenticator := append(middlewareAuthenticator, auth.NewEmailAuthenticator(authService))

		auditChecker := version.NewDefaultAuditChecker(cfg.Security.AuditCheckURL, metadata.InstallationID)
		defer auditChecker.Close()
		if version.Version != version.UnreleasedVersion {
			auditChecker.StartPeriodicCheck(ctx, cfg.Security.AuditCheckInterval, logger)
		}

		allowForeign, err := cmd.Flags().GetBool(mismatchedReposFlagName)
		if err != nil {
			fmt.Printf("%s: %s\n", mismatchedReposFlagName, err)
			os.Exit(1)
		}
		if !allowForeign {
			checkRepos(ctx, logger, authMetadataManager, blockStore, c)
		}

		// update health info with installation ID
		httputil.SetHealthHandlerInfo(metadata.InstallationID)

		// start API server
		var oauthConfig *oauth2.Config
		var oidcProvider *oidc.Provider
		if cfg.Auth.OIDC.Enabled {
			logger.
				WithField("feature", "oidc").
				Warn("Enabling OIDC on lakeFS server, but this functionality is deprecated")
			oidcProvider, err = oidc.NewProvider(
				ctx,
				cfg.Auth.OIDC.URL,
			)
			if err != nil {
				logger.WithError(err).Fatal("Failed to initialize OIDC provider")
			}
			scopes := []string{oidc.ScopeOpenID, "profile"}

			scopes = append(scopes, cfg.Auth.OIDC.AdditionalScopeClaims...)
			oauthConfig = &oauth2.Config{
				ClientID:     cfg.Auth.OIDC.ClientID,
				ClientSecret: cfg.Auth.OIDC.ClientSecret,
				RedirectURL:  strings.TrimSuffix(cfg.Auth.OIDC.CallbackBaseURL, "/") + api.BaseURL + "/oidc/callback",
				Endpoint:     oidcProvider.Endpoint(),
				Scopes:       scopes,
			}
		}
		apiHandler := api.Serve(
			cfg,
			c,
			middlewareAuthenticator,
			controllerAuthenticator,
			authService,
			blockStore,
			authMetadataManager,
			migrator,
			bufferedCollector,
			cloudMetadataProvider,
			actionsService,
			auditChecker,
			logger.WithField("service", "api_gateway"),
			emailer,
			templater,
			cfg.Gateways.S3.DomainNames,
			cfg.UISnippets(),
			oidcProvider,
			oauthConfig,
			upload.DefaultPathProvider,
		)

		// init gateway server
		var s3FallbackURL *url.URL
		if cfg.Gateways.S3.FallbackURL != "" {
			s3FallbackURL, err = url.Parse(cfg.Gateways.S3.FallbackURL)
			if err != nil {
				logger.WithError(err).Fatal("Failed to parse s3 fallback URL")
			}
		}

		lakefsBaseURL := cfg.Email.LakefsBaseURL
		if lakefsBaseURL != "" {
			_, err := url.Parse(lakefsBaseURL)
			if err != nil {
				logger.WithError(err).Warn("Failed to parse configured lakefs base url for email")
			}
		}

		s3gatewayHandler := gateway.NewHandler(
			cfg.Gateways.S3.Region,
			c,
			multipartTracker,
			blockStore,
			authService,
			cfg.Gateways.S3.DomainNames,
			bufferedCollector,
			upload.DefaultPathProvider,
			s3FallbackURL,
			cfg.Logging.AuditLogLevel,
			cfg.Logging.TraceRequestHeaders,
		)

		bufferedCollector.Start(ctx)
		defer bufferedCollector.Close()

		bufferedCollector.CollectEvent(stats.Event{Class: "global", Name: "run"})

		logger.WithField("listen_address", cfg.ListenAddress).Info("starting HTTP server")
		server := &http.Server{
			Addr:              cfg.ListenAddress,
			ReadHeaderTimeout: time.Minute,
			Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				// If the request has the S3 GW domain (exact or subdomain) - or carries an AWS sig, serve S3GW
				if httputil.HostMatches(request, cfg.Gateways.S3.DomainNames) ||
					httputil.HostSubdomainOf(request, cfg.Gateways.S3.DomainNames) ||
					sig.IsAWSSignedRequest(request) {
					s3gatewayHandler.ServeHTTP(writer, request)
					return
				}

				// Otherwise, serve the API handler
				apiHandler.ServeHTTP(writer, request)
			}),
		}

		actionsService.SetEndpoint(server)

		go func() {
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to listen on %s: %v\n", cfg.ListenAddress, err)
				os.Exit(1)
			}
		}()

		gracefulShutdown(ctx, server)
	},
}

// checkRepos iterates on all repos and validates that their settings are correct.
func checkRepos(ctx context.Context, logger logging.Logger, authMetadataManager auth.MetadataManager, blockStore block.Adapter, c *catalog.Catalog) {
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
			repos, hasMore, err = c.ListRepositories(ctx, -1, "", next)
			if err != nil {
				logger.WithError(err).Fatal("Checking existing repositories failed")
			}

			adapterStorageType := blockStore.BlockstoreType()
			for _, repo := range repos {
				nsURL, err := url.Parse(repo.StorageNamespace)
				if err != nil {
					logger.WithError(err).Fatalf("Failed to parse repository %s namespace '%s'", repo.Name, repo.StorageNamespace)
				}
				repoStorageType, err := block.GetStorageType(nsURL)
				if err != nil {
					logger.WithError(err).Fatalf("Failed to parse to parse storage type '%s'", nsURL)
				}

				checkForeignRepo(repoStorageType, logger, adapterStorageType, repo.Name)
				checkMetadataPrefix(ctx, repo, logger, blockStore, repoStorageType)

				next = repo.Name
			}
		}
	}
}

func scheduleCleanupJobs(ctx context.Context, s *gocron.Scheduler, c *catalog.Catalog) error {
	// delete expired link addresses
	const deleteExpiredAddressPeriod = 3
	job1, err := s.Every(deleteExpiredAddressPeriod * ref.LinkAddressTime).Do(func() {
		c.DeleteExpiredLinkAddresses(ctx)
	})
	if err != nil {
		return err
	}
	job1.SingletonMode()

	// delete expired tracked physical addresses
	const (
		deleteTrackedLowerTimeSec = 50
		deleteTrackedUpperTimeSec = 80
	)
	job2, err := s.EveryRandom(deleteTrackedLowerTimeSec, deleteTrackedUpperTimeSec).Minute().Do(func() {
		c.DeleteTrackedPhysicalAddresses(ctx)
	})
	if err != nil {
		return err
	}
	job2.SingletonMode()
	return nil
}

func getScheduler() *gocron.Scheduler {
	return gocron.NewScheduler(time.UTC)
}

// checkMetadataPrefix checks for non-migrated repos of issue #2397 (https://github.com/treeverse/lakeFS/issues/2397)
func checkMetadataPrefix(ctx context.Context, repo *catalog.Repository, logger logging.Logger, adapter block.Adapter, repoStorageType block.StorageType) {
	if repoStorageType != block.StorageTypeGS &&
		repoStorageType != block.StorageTypeAzure {
		return
	}

	const dummyFile = "dummy"
	if _, err := adapter.Get(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		Identifier:       dummyFile,
	}, -1); err != nil {
		logger.WithFields(logging.Fields{
			"path":              dummyFile,
			"storage_namespace": repo.StorageNamespace,
		}).Fatal("Can't find dummy file in storage namespace, did you run the migration? " +
			"(https://docs.lakefs.io/reference/upgrade.html#data-migration-for-version-v0500)")
	}
}

// checkForeignRepo checks whether a repo storage namespace matches the block adapter.
// A foreign repo is a repository which namespace doesn't match the current block adapter.
// A foreign repo might exist if the lakeFS instance configuration changed after a repository was
// already created. The behaviour of lakeFS for foreign repos is undefined and should be blocked.
func checkForeignRepo(repoStorageType block.StorageType, logger logging.Logger, adapterStorageType, repoName string) {
	if adapterStorageType != repoStorageType.BlockstoreType() {
		logger.Fatalf("Mismatched adapter detected. lakeFS started with adapter of type '%s', but repository '%s' is of type '%s'",
			adapterStorageType, repoName, repoStorageType.BlockstoreType())
	}
}

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

│
│ For support or any other question,
│     join our Slack channel https://docs.lakefs.io/slack
│

`

func printWelcome(w io.Writer) {
	_, _ = fmt.Fprint(w, runBanner)
	_, _ = fmt.Fprintf(w, "Version %s\n\n", version.Version)
}

const localWarningBanner = `
WARNING!

Using %s.  This is suitable only for testing! It is NOT SUPPORTED for production.
`

func printLocalWarning(w io.Writer, msg string) {
	_, _ = fmt.Fprintf(w, localWarningBanner, msg)
}

func gracefulShutdown(ctx context.Context, services ...Shutter) {
	_, _ = fmt.Fprintf(os.Stderr, "lakeFS %s - Up and running (^C to shutdown)...\n", version.Version)
	printWelcome(os.Stderr)
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
func enableKVParamsMetrics(p params.Config) params.Config {
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
