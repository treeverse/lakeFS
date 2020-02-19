package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/treeverse/lakefs/api"

	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

/*
commands:
lakectl config init [--path]

lakectl repo list
lakectl repo create lakefs://myrepo
lakectl repo delete lakefs://myrepo (confirm)
lakectl repo show lakefs://myrepo

lakectl branch list lakefs://myrepo
lakectl branch create lakefs://myrepo@feature-new --source lakefs://myrepo@master
lakectl branch show lakefs://myrepo@feature-new
lakectl branch delete lakefs://myrepo@feature-new
lakectl branch revert lakefs://myrepo@feature-new  [--commit 123456] [--path /]

lakectl fs ls lakefs://myrepo@master/collections/ [--from "collections/file.csv"]
lakectl fs stat lakefs://myrepo@master/collections/file.csv
lakectl fs cat lakefs://myrepo@master/collections/file.csv
lakectl fs upload /path/to/local/file lakefs://myrepo@master/collections/file.csv
lakectl fs rm lakefs://myrepo@master/collections/file.csv [--recursive]

lakectl commit lakefs://myrepo@master --message "commit message"
lakectl diff lakefs://myrepo@master other-branch [--path /]
lakectl checkout lakefs://myrepo@master/collections/file.csv
lakectl reset lakefs://myrepo@master
lakectl merge lakefs://myrepo@my-branch lakefs://myrepo@master
*/

const (
	DefaultConfigFileDirectory = "~/.lakefs"
	DefaultConfigFileBareName  = "config"
	DefaultConfigFileName      = "config.yaml"
	DefaultConfigFilePath      = "~/.lakefs/config.yaml"

	ConfigAccessKeyId       = "credentials.access_key_id"
	ConfigSecretAccessKey   = "credentials.secret_access_key"
	ConfigServerEndpointUrl = "server.endpoint_url"
)

var cfgFile string

// rootCmd represents the base command when called without any sub-commands
var rootCmd = &cobra.Command{
	Use:   "lakectl",
	Short: "A cli tool to explore manage and work withl lakeFS",
	Long: `lakeFS is data lake management solution, allowing Git-like semantics over common object stores

lakectl is a CLI tool allowing exploration and manipulation of a lakeFS environment`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if strings.EqualFold(cmd.Use, "config") && len(args) > 0 && strings.EqualFold(args[0], "init") {
			initConfig(false)
		} else {
			initConfig(true)
		}
	},
}

func getClient() (api.Client, error) {
	return api.NewClient(
		viper.GetString(ConfigServerEndpointUrl),
		viper.GetString(ConfigAccessKeyId),
		viper.GetString(ConfigSecretAccessKey),
	)
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
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", fmt.Sprintf("config file (default is %s)", DefaultConfigFilePath))
}

// initConfig reads in config file and ENV variables if set.
func initConfig(readConf bool) {

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		cfgHome, err := homedir.Expand(DefaultConfigFileDirectory)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".lakefs" (without extension).
		viper.AddConfigPath(cfgHome)
		viper.SetConfigName(DefaultConfigFileBareName)
	}

	viper.AutomaticEnv() // read in environment variables that match

	// if we're running "config init", don't read the config first
	if !readConf {
		return
	}

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if desired
			fmt.Fprintf(os.Stderr, "config file not found, please run \"lakectl config init\" to create one\n%s\n", err.Error())
			os.Exit(1)
		} else {
			// Config file was found but another error was produced
			panic(fmt.Sprintf("error reading configuration file: %v", err))
		}
	}
}
