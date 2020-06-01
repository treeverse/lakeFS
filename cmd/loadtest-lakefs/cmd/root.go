package cmd

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/schollz/progressbar/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/loadtesting"
	"os"
	"strings"
	"time"
)

const (
	ConfigAccessKeyId       = "credentials.access_key_id"
	ConfigSecretAccessKey   = "credentials.secret_access_key"
	ConfigServerEndpointUrl = "server.endpoint_url"
)

const (
	DurationFlag  = "duration"
	FrequencyFlag = "freq"
	RepoNameFlag  = "repo"
	CleanFlag     = "clean"
)

var (
	cfgFile string
)

// rootCmd represents the base command when called without any sub-commands
var rootCmd = &cobra.Command{
	Use:   "loadtest-lakefs",
	Short: "Run a load test on a lakeFS instance",
	Long:  `You can either run the tests on a running lakeFS instance, or choose to start a dedicated lakeFS server as part of the test.`,
	Run: func(cmd *cobra.Command, args []string) {

		repoName, err := cmd.Flags().GetString(RepoNameFlag)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		durationInSec, _ := cmd.Flags().GetInt(DurationFlag)
		requestsPerSeq, _ := cmd.Flags().GetInt(FrequencyFlag)
		clean, _ := cmd.Flags().GetBool(CleanFlag)
		progressBar(durationInSec)
		err = loadtesting.LoadTest(loadtesting.LoadTesterConfig{
			FreqPerSecond: requestsPerSeq,
			Duration:      time.Duration(durationInSec) * time.Second,
			RepoName:      repoName,
			DeleteRepo:    clean,
			Credentials: model.Credential{
				AccessKeyId:     viper.GetString(ConfigAccessKeyId),
				AccessSecretKey: viper.GetString(ConfigSecretAccessKey),
			},
			ServerAddress: viper.GetString(ConfigServerEndpointUrl),
		})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
	Version: config.Version,
}

func progressBar(durationInSec int) {
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
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	cobra.OnInitialize(initConfig)
	rootCmd.Flags().StringVarP(&cfgFile, "config", "c", "", "Config file (default is $HOME/.lakectl.yaml)")
	rootCmd.Flags().StringP(RepoNameFlag, "r", "", "Existing lakeFS repo name to use. Leave empty to create a dedicated repo")
	rootCmd.Flags().Bool(CleanFlag, false, "Delete repo at the end of the test")
	rootCmd.Flags().IntP(FrequencyFlag, "f", 5, "Number of requests to send per second")
	rootCmd.Flags().IntP(DurationFlag, "d", 30, "Duration of test, in seconds")
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

	viper.SetEnvPrefix("LAKECTL")
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
