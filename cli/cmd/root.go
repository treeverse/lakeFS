package cmd

import (
	"fmt"
	"os"

	"github.com/treeverse/lakefs/api"

	"github.com/treeverse/lakefs/api/service"

	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

/*
commands:
lakectl config init

lakectl repo list
lakectl repo create --repo foobar
lakectl repo delete --repo foobar (confirm)
lakectl repo show --repo foobar

lakectl branch create --repo foorbar --branch feature-new --source master
lakectl branch delete --repo foobar --branch feature-new
lakectl branch list --repo foobar
lakectl branch show --repo foobar --branch master

lakectl fs ls foobar master collections/ [--from "collections/some_key"]
lakectl fs stat foobar master collections/some_key
lakectl fs cat foobar master collections/some_key
lakectl fs upload /path/to/local/file foobar master collections/some_key
lakectl fs rm foobar master collections/some_key

lakectl commit foobar master -m "commit message"
lakectl diff foobar master other-branch --path /
lakectl checkout foobar master --path collections/some_key
lakectl reset foobar master
lakectl merge foobar --source my-branch --destination master
*/

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "lakectl",
	Short: "A cli tool to explore manage and work withl lakeFS",
	Long: `lakeFS is data lake management solution, allowing Git-like semantics over common object stores

lakectl is a CLI tool allowing exploration and manipulation of a lakeFS environment`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

func getClient() (service.APIServerClient, error) {
	// TODO: pass config to setup client and credentials
	return api.NewClient("localhost:8001", "AKIAJKLO4PDKEBQUDHYQ", "aQ+afKWc5IPG+r0P3HVmPSjQN7ehyxwJw/wp9AIz")
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.lakefs/config")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".lakefs" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".lakefs/config")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
