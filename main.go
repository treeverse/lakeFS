package main

import (
	"fmt"
	"os"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/db"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/permissions"
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

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "initialize a LakeFS instance, and setup an admin credential",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)
		adb := conf.BuildAuthDatabase()

		userEmail, _ := cmd.Flags().GetString("email")
		userFullName, _ := cmd.Flags().GetString("full-name")

		authService := auth.NewDBAuthService(adb)
		user := &model.User{
			Email:    userEmail,
			FullName: userFullName,
		}
		err := authService.CreateUser(user)
		if err != nil {
			panic(err)
		}

		role := &model.Role{
			DisplayName: "Admin",
		}

		err = authService.CreateRole(role)
		if err != nil {
			panic(err)
		}
		policies := []*model.Policy{
			{
				Permission: string(permissions.ManageRepos),
				Arn:        "arn:treeverse:repos:::*",
			},
			{
				Permission: string(permissions.ReadRepo),
				Arn:        "arn:treeverse:repos:::*",
			},
			{
				Permission: string(permissions.WriteRepo),
				Arn:        "arn:treeverse:repos:::*",
			},
		}
		for _, policy := range policies {
			err = authService.AssignPolicyToRole(role.Id, policy)
			if err != nil {
				panic(err)
			}
		}

		err = authService.AssignRoleToUser(role.Id, user.Id)
		if err != nil {
			panic(err)
		}

		creds, err := authService.CreateUserCredentials(user)
		if err != nil {
			panic(err)
		}

		fmt.Printf("credentials:\naccess key id: %s\naccess secret key: %s\n", creds.AccessKeyId, creds.AccessSecretKey)
		return nil
	},
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run a LakeFS instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)
		mdb := conf.BuildMetadataDatabase()
		adb := conf.BuildAuthDatabase()

		// init index
		meta := index.NewDBIndex(mdb)

		// init mpu manager
		mpu := index.NewDBMultipartManager(store.NewDBStore(mdb))

		// init block store
		blockStore := conf.BuildBlockAdapter()

		// init authentication
		authService := auth.NewDBAuthService(adb)

		// start API server
		apiServer := api.NewServer(meta, mpu, blockStore, authService)
		go func() {
			panic(apiServer.Serve(conf.GetAPIListenAddress()))
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
		)

		panic(gatewayServer.Listen())

		return nil
	},
}

var treeCmd = &cobra.Command{
	Use:   "tree",
	Short: "dump the entire filesystem tree for the given repository and branch to stdout",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)
		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		mdb := conf.BuildMetadataDatabase()
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

		// start with lakefs_index
		mdb := conf.BuildMetadataDatabase()
		_, err := mdb.Transact(func(tx db.Tx) (interface{}, error) {
			err := db.MigrateSchemaAll(tx, "lakefs_index")
			return nil, err
		}, db.WithLogger(logging.Dummy()))
		if err != nil {
			panic(err)
		}

		// then auth db
		adb := conf.BuildAuthDatabase()
		_, err = adb.Transact(func(tx db.Tx) (interface{}, error) {
			err := db.MigrateSchemaAll(tx, "lakefs_auth")
			return nil, err
		}, db.WithLogger(logging.Dummy()))
		if err != nil {
			panic(err)
		}

		return nil
	},
}

var rootCmd = &cobra.Command{
	Use:   "lakefs [sub-command]",
	Short: "lakefs is a data lake management platform",
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
