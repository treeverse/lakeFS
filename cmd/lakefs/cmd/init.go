package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/auth/model"
)

// initCmd represents the init command
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a LakeFS instance, and setup an admin credential",
	Run: func(cmd *cobra.Command, args []string) {
		dbPool := cfg.BuildDatabaseConnection()
		defer func() { _ = dbPool.Close() }()

		userName, _ := cmd.Flags().GetString("user-name")

		authService := auth.NewDBAuthService(dbPool, crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()))
		user := &model.User{
			CreatedAt:   time.Now(),
			DisplayName: userName,
		}
		creds, err := api.SetupAdminUser(authService, user)
		if err != nil {
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(context.Background())
		stats := cfg.BuildStats(userName)
		go stats.Run(ctx)
		stats.Collect("global", "init")

		fmt.Printf("credentials:\naccess key id: %s\naccess secret key: %s\n", creds.AccessKeyId, creds.AccessSecretKey)

		cancelFn()
		<-stats.Done()
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
	initCmd.Flags().String("user-name", "", "display name for the user (e.g. \"jane.doe\")")
	_ = initCmd.MarkFlagRequired("user-name")
}
