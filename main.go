package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/stats"
)

const (
	DefaultInstallationID   = "anon@example.com"
	gracefulShutdownTimeout = 30 * time.Second
)

func setupConf(cmd *cobra.Command) *config.Config {
	confFile, err := cmd.Flags().GetString("config")
	if err != nil {
		panic(err)
	}
	if len(confFile) > 0 {
		return config.NewFromFile(confFile)
	}
	return config.New()
}

func GetInstallationID(authService auth.Service) string {
	user, err := authService.GetFirstUser()
	if err != nil {
		return DefaultInstallationID
	}
	return user.Email
}

func getStats(conf *config.Config, installationID string) *stats.BufferedCollector {
	sender := stats.NewDummySender()
	if conf.GetStatsEnabled() {
		sender = stats.NewHTTPSender(installationID, uuid.New().String(), conf.GetStatsAddress(), time.Now)
	}
	return stats.NewBufferedCollector(
		stats.WithSender(sender),
		stats.WithFlushInterval(conf.GetStatsFlushInterval()))
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "initialize a LakeFS instance, and setup an admin credential",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)
		adb := conf.ConnectAuthDatabase()

		userEmail, _ := cmd.Flags().GetString("email")
		userFullName, _ := cmd.Flags().GetString("full-name")

		authService := auth.NewDBAuthService(adb, crypt.NewSecretStore(conf.GetAuthEncryptionSecret()))
		user := &model.User{
			Email:    userEmail,
			FullName: userFullName,
		}
		creds, err := api.SetupAdminUser(authService, user)
		if err != nil {
			panic(err)
		}

		ctx, cancelFn := context.WithCancel(context.Background())
		stats := getStats(conf, userEmail)
		go stats.Run(ctx)
		stats.Collect("global", "init")

		fmt.Printf("credentials:\naccess key id: %s\naccess secret key: %s\n", creds.AccessKeyId, creds.AccessSecretKey)

		cancelFn()
		<-stats.Done()
		return nil
	},
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run a LakeFS instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)
		mdb := conf.ConnectMetadataDatabase()
		adb := conf.ConnectAuthDatabase()
		migrator := db.NewDatabaseMigrator().
			AddDB(db.SchemaMetadata, mdb).
			AddDB(db.SchemaAuth, adb)

		// init index
		meta := index.NewDBIndex(mdb)

		// init mpu manager
		mpu := index.NewDBMultipartManager(store.NewDBStore(mdb))

		// init block store
		blockStore := conf.BuildBlockAdapter()

		// init authentication
		authService := auth.NewDBAuthService(adb, crypt.NewSecretStore(conf.GetAuthEncryptionSecret()))

		ctx, cancelFn := context.WithCancel(context.Background())
		stats := getStats(conf, GetInstallationID(authService))

		// start API server
		apiServer := api.NewServer(meta, mpu, blockStore, authService, stats, migrator)

		done := make(chan bool, 1)
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt)

		go func() {
			if err := apiServer.Serve(conf.GetAPIListenAddress()); err != nil && err != http.ErrServerClosed {
				log.Fatalf("API server failed to listen on %s: %v", conf.GetAPIListenAddress(), err)
			}
		}()

		// init gateway server
		gatewayServer := gateway.NewServer(
			conf.GetS3GatewayRegion(),
			meta,
			blockStore,
			authService,
			mpu,
			conf.GetS3GatewayListenAddress(),
			conf.GetS3GatewayDomainName(),
			stats,
		)

		go stats.Run(ctx)
		stats.Collect("global", "run")

		go func() {
			if err := gatewayServer.Listen(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("Gateway server failed to listen on %s: %v", conf.GetS3GatewayListenAddress(), err)
			}
		}()

		go gracefulShutdown(apiServer, gatewayServer, quit, done)

		<-done
		cancelFn()
		<-stats.Done()
		log.Println("Bye")
		return nil
	},
}

func gracefulShutdown(apiServer *api.Server, gatewayServer *gateway.Server, quit <-chan os.Signal, done chan<- bool) {
	log.Println("Control-C to shutdown")
	<-quit
	log.Println("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
	defer cancel()

	if err := apiServer.Shutdown(ctx); err != nil {
		log.Fatalf("Cloud not shutdown the api server: %v", err)
	}

	if err := gatewayServer.Shutdown(ctx); err != nil {
		log.Fatalf("Cloud not shutdown the gateway server: %v", err)
	}

	close(done)
}

var treeCmd = &cobra.Command{
	Use:   "tree",
	Short: "dump the entire filesystem tree for the given repository and branch to stdout",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)
		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		mdb := conf.ConnectMetadataDatabase()
		meta := index.NewDBIndex(mdb)

		err := meta.Tree(repo, branch)
		if err != nil {
			panic(err)
		}
		return nil
	},
}

var setupdbCmd = &cobra.Command{
	Use:   "setupdb",
	Short: "run schema and data migrations on a fresh database",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)
		migrator := db.NewDatabaseMigrator().
			AddDB(db.SchemaMetadata, conf.ConnectMetadataDatabase()).
			AddDB(db.SchemaAuth, conf.ConnectAuthDatabase())
		err := migrator.Migrate()
		if err != nil {
			panic(err)
		}

		return nil
	},
}

var rootCmd = &cobra.Command{
	Use:     "lakefs [sub-command]",
	Short:   "lakefs is a data lake management platform",
	Version: config.Version,
}

func init() {
	rootCmd.PersistentFlags().StringP("config", "c", "", "configuration file path")

	treeCmd.Flags().StringP("repo", "r", "", "repository to list")
	treeCmd.Flags().StringP("branch", "b", "", "branch to list")
	_ = treeCmd.MarkFlagRequired("repo")
	_ = treeCmd.MarkFlagRequired("branch")

	initCmd.Flags().String("email", "", "E-mail of the user to generate")
	initCmd.Flags().String("full-name", "", "Full name of the user to generate")
	_ = initCmd.MarkFlagRequired("email")
	_ = initCmd.MarkFlagRequired("full-name")

	rootCmd.AddCommand(treeCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(setupdbCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
