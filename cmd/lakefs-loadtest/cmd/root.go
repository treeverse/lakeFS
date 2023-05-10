package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	ConfigAccessKeyID       = "credentials.access_key_id"
	ConfigSecretAccessKey   = "credentials.secret_access_key"
	ConfigServerEndpointURL = "server.endpoint_url"

	DefaultServerEndpointURL = "http://localhost:8000"
)

var cfgFile string

// rootCmd represents the base command when called without any sub-commands
var rootCmd = &cobra.Command{
	Use:     "lakefs-loadtest",
	Short:   "Run a loadtest on a lakeFS instance.",
	Version: version.Version,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

//nolint:gochecknoinits
func init() {
	cobra.OnInitialize(initConfig)
	runCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "Config file (default is $HOME/.lakectl.yaml)")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetDefault(ConfigServerEndpointURL, DefaultServerEndpointURL)

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
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".lakectl")
	}

	viper.SetEnvPrefix("LAKEFS_LOADTEST")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // support nested config
	viper.AutomaticEnv()                                   // read in environment variables that match
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
		fmt.Println("Error while reading config file:", viper.ConfigFileUsed(), "-", err)
	} else {
		// err is viper.ConfigFileNotFoundError
		fmt.Println("Config file not found. Will try to use environment variables.")
	}
}
