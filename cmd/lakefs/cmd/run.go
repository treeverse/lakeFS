package cmd

import (
	"context"
	"log"
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
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/store"
)

const gracefulShutdownTimeout = 30 * time.Second

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a LakeFS instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		mdb := cfg.ConnectMetadataDatabase()
		adb := cfg.ConnectAuthDatabase()
		migrator := db.NewDatabaseMigrator().
			AddDB(db.SchemaMetadata, mdb).
			AddDB(db.SchemaAuth, adb)

		// init index
		meta := index.NewDBIndex(mdb)

		// init mpu manager
		mpu := index.NewDBMultipartManager(store.NewDBStore(mdb))

		// init block store
		blockStore := cfg.BuildBlockAdapter()

		// init authentication
		authService := auth.NewDBAuthService(adb, crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()))

		ctx, cancelFn := context.WithCancel(context.Background())
		stats := config.GetStats(cfg, config.GetInstallationID(authService))

		// start API server
		apiServer := api.NewServer(meta, mpu, blockStore, authService, stats, migrator)

		done := make(chan bool, 1)
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt)

		go func() {
			if err := apiServer.Serve(cfg.GetAPIListenAddress()); err != nil && err != http.ErrServerClosed {
				log.Fatalf("API server failed to listen on %s: %v", cfg.GetAPIListenAddress(), err)
			}
		}()

		// init gateway server
		gatewayServer := gateway.NewServer(
			cfg.GetS3GatewayRegion(),
			meta,
			blockStore,
			authService,
			mpu,
			cfg.GetS3GatewayListenAddress(),
			cfg.GetS3GatewayDomainName(),
			stats,
		)

		go stats.Run(ctx)
		stats.Collect("global", "run")

		go func() {
			if err := gatewayServer.Listen(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("Gateway server failed to listen on %s: %v", cfg.GetS3GatewayListenAddress(), err)
			}
		}()

		go gracefulShutdown(apiServer, gatewayServer, quit, done)

		<-done
		cancelFn()
		<-stats.Done()
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
