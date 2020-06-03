package cmd

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/config"
	"os"
	"strings"
	"time"
)

const (
	ConfigAccessKeyId       = "credentials.access_key_id"
	ConfigSecretAccessKey   = "credentials.secret_access_key"
	ConfigServerEndpointUrl = "server.endpoint_url"
)

var (
	cfgFile string
)

// rootCmd represents the base command when called without any sub-commands
var rootCmd = &cobra.Command{
	Use:     "lakefs-bench",
	Short:   "Run a benchmark on a lakeFS instance.",
	Version: config.Version,
}

func progressBar(duration time.Duration) {
	durationInSec := int(duration.Seconds())
	progress := progressbar.NewOptions(durationInSec, progressbar.OptionSetPredictTime(false))
	go func() {
		for i := 0; i < durationInSec; i++ {
			_ = progress.Add(1)
			time.Sleep(time.Second)
		}
		_ = progress.Clear()
	}()
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

func init() {
	cobra.OnInitialize(initConfig)
	runCmd.Flags().StringVarP(&cfgFile, "config", "c", "", "Config file (default is $HOME/.lakectl.yaml)")
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
		viper.SetConfigType("yaml")
		viper.SetConfigName(".lakectl")
	}

	viper.SetEnvPrefix("LOADTEST")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // support nested config
	viper.AutomaticEnv()                                   // read in environment variables that match
	cfgFileErr := viper.ReadInConfig()
	if cfgFileErr != nil {
		if _, ok := cfgFileErr.(viper.ConfigFileNotFoundError); ok {
			// specific message in case the file doesn't not found
			fmt.Printf("config file not found, please run \"lakectl config\" to create one\n%s\n", cfgFileErr)
			os.Exit(1)
		} else {
			// other errors while reading the config file
			fmt.Printf("error reading configuration file: %v\n", cfgFileErr)
			os.Exit(1)
		}
	}
}
