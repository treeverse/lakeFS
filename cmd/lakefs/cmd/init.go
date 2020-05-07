package cmd

import (
	"context"
	"fmt"
	"os"

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
		adb := cfg.ConnectAuthDatabase()

		userEmail, _ := cmd.Flags().GetString("email")
		userFullName, _ := cmd.Flags().GetString("full-name")

		authService := auth.NewDBAuthService(adb, crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()))
		user := &model.User{
			Email:    userEmail,
			FullName: userFullName,
		}
		creds, err := api.SetupAdminUser(authService, user)
		if err != nil {
			fmt.Printf("Failed to setup admin user: %s\n", err)
			os.Exit(1)
		}

		ctx, cancelFn := context.WithCancel(context.Background())
		stats := cfg.BuildStats(userEmail)
		go stats.Run(ctx)
		stats.Collect("global", "init")

		fmt.Printf("credentials:\naccess key id: %s\naccess secret key: %s\n", creds.AccessKeyId, creds.AccessSecretKey)

		cancelFn()
		<-stats.Done()
	},
}

func init() {
	rootCmd.AddCommand(initCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// initCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// initCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	initCmd.Flags().String("email", "", "E-mail of the user to generate")
	initCmd.Flags().String("full-name", "", "Full name of the user to generate")
	_ = initCmd.MarkFlagRequired("email")
	_ = initCmd.MarkFlagRequired("full-name")
}
