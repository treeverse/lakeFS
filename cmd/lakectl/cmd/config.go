package cmd

import (
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/mitchellh/go-homedir"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "create/update local lakeFS configuration",
	Run: func(cmd *cobra.Command, args []string) {
		if viper.ConfigFileUsed() == "" {
			// Find home directory.
			home, err := homedir.Dir()
			if err != nil {
				DieErr(err)
			}
			// Setup default config file
			viper.SetConfigFile(filepath.Join(home, ".lakectl.yaml"))
		}
		fmt.Printf("Config file %s will be used\n", viper.ConfigFileUsed())

		// get user input
		questions := []struct {
			Key    string
			Prompt *promptui.Prompt
		}{
			{Key: ConfigAccessKeyID, Prompt: &promptui.Prompt{Label: "Access key ID"}},
			{Key: ConfigSecretAccessKey, Prompt: &promptui.Prompt{Label: "Secret access key"}},
			{Key: ConfigServerEndpointURL, Prompt: &promptui.Prompt{Label: "Server endpoint URL", Validate: func(rawurl string) error {
				_, err := url.ParseRequestURI(rawurl)
				return err
			}}},
			{Key: ConfigDefaultStorageNamespace, Prompt: &promptui.Prompt{Label: "Default storage namespace"}},
		}
		for _, question := range questions {
			question.Prompt.Default = viper.GetString(question.Key)
			val, err := question.Prompt.Run()
			if err != nil {
				DieErr(err)
			}
			viper.Set(question.Key, val)
		}

		err := viper.SafeWriteConfig()
		if err != nil {
			err = viper.WriteConfig()
		}
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(configCmd)
}
