package cmd

import (
	"fmt"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Create/update local lakeFS configuration",
	Run: func(cmd *cobra.Command, args []string) {
		if viper.ConfigFileUsed() == "" {
			Die("No config file was specified", 1)
		}
		fmt.Printf("Config file %s will be updated\n", viper.ConfigFileUsed())

		// get user input
		questions := []struct {
			Key    string
			Prompt *promptui.Prompt
		}{
			{Key: ConfigAccessKeyId, Prompt: &promptui.Prompt{Label: "Access key ID"}},
			{Key: ConfigSecretAccessKey, Prompt: &promptui.Prompt{Label: "Secret access key"}},
			{Key: ConfigServerEndpointUrl, Prompt: &promptui.Prompt{Label: "Server endpoint URL", Validate: promptuiValidateURL}},
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

func init() {
	rootCmd.AddCommand(configCmd)
}
