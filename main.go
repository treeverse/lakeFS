package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/gateway"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/permissions"
	"os"
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
		kv := conf.BuildStore()

		userId, _ := cmd.Flags().GetString("user-id")
		userEmail, _ := cmd.Flags().GetString("user-email")
		userFullName, _ := cmd.Flags().GetString("user-name")

		authService := auth.NewKVAuthService(kv)
		err := authService.CreateUser(&model.User{
			Id:       userId,
			Email:    userEmail,
			FullName: userFullName,
		})
		if err != nil {
			panic(err)
		}

		err = authService.CreateRole(&model.Role{
			Id:   "admin",
			Name: "Admin",
			Policies: []*model.Policy{
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
			},
		})
		if err != nil {
			panic(err)
		}

		err = authService.AssignRoleToUser("admin", userId)
		if err != nil {
			panic(err)
		}

		creds, err := authService.CreateUserCredentials(&model.User{Id: userId})
		if err != nil {
			panic(err)
		}

		fmt.Printf("credentials:\naccess key id: %s\naccess secret key: %s\n", creds.GetAccessKeyId(), creds.GetAccessSecretKey())
		return nil
	},
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run a LakeFS instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)

		kv := conf.BuildStore()

		// init index
		meta := index.NewKVIndex(store.NewKVStore(kv))

		// init mpu manager
		mpu := index.NewKVMultipartManager(store.NewKVStore(kv))

		// init block store
		blockStore := conf.BuildBlockAdapter()

		// init authentication
		authService := auth.NewKVAuthService(kv)

		// start API server
		apiServer := api.NewServer(meta, mpu, blockStore, authService)
		go func() {
			panic(apiServer.Serve(conf.GetAPIListenHost(), conf.GetAPIListenPort()))
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
		if httputil.IsPlayback() {
			httputil.DoTestRun(gatewayServer.Server.Handler)
		} else {
			panic(gatewayServer.Listen())
		}
		return nil
	}}

var keysCmd = &cobra.Command{
	Use:   "keys",
	Short: "dump all metadata keys to stdout",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)

		kv := conf.BuildStore()

		_, err := kv.ReadTransact(func(q db.ReadQuery) (i interface{}, err error) {
			iter, closer := q.RangeAll()
			defer closer()
			for iter.Advance() {
				item, _ := iter.Get()
				fmt.Printf("%s\n", db.KeyFromBytes(item.Key))
			}
			return nil, nil
		})

		return err
	},
}

var treeCmd = &cobra.Command{
	Use:   "tree",
	Short: "dump the entire filesystem tree for the given repository and branch to stdout",
	RunE: func(cmd *cobra.Command, args []string) error {
		conf := setupConf(cmd)
		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		meta := index.NewKVIndex(store.NewKVStore(conf.BuildStore()))

		err := meta.Tree(repo, branch)
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

	initCmd.Flags().String("user-id", "", "ID of the user to be generated")
	initCmd.Flags().String("user-email", "", "E-mail of the user to generate")
	initCmd.Flags().String("user-name", "", "Full name of the user to generate")
	_ = initCmd.MarkFlagRequired("user-id")
	_ = initCmd.MarkFlagRequired("user-email")
	_ = initCmd.MarkFlagRequired("user-name")

	rootCmd.AddCommand(treeCmd)
	rootCmd.AddCommand(keysCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(initCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
