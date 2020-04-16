/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/mitchellh/go-homedir"

	"github.com/spf13/viper"

	"github.com/spf13/cobra"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "manage local lakefs configuration",
	Run: func(cmd *cobra.Command, args []string) {
		// create directory if it doesn't exist
		configDir, _ := homedir.Expand(DefaultConfigFileDirectory)
		configFile, _ := homedir.Expand(DefaultConfigFilePath)
		err := os.MkdirAll(configDir, 0755)
		if err != nil {
			DieErr(err)
		}

		f, err := os.OpenFile(configFile, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			DieErr(err)
		}
		err = f.Close()
		if err != nil {
			DieErr(err)
		}
		// write config
		setConfigVal(ConfigAccessKeyId, "access key id ", prompt)
		setConfigVal(ConfigSecretAccessKey, "secret access key", prompt)
		setConfigVal(ConfigServerEndpointUrl, "endpoint URL", promptUrl)

		err = viper.WriteConfigAs(configFile)
		if err != nil {
			DieErr(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
}

type promptFunc func(question string) (string, error)

func setConfigVal(key, promptStr string, prompt promptFunc) {
	currentAccessKeyId := viper.Get(key)

	if currentAccessKeyId != nil {
		promptStr += fmt.Sprintf(" [%s]", currentAccessKeyId)
	}
	accessKeyId, err := prompt(promptStr)
	if err != nil {
		DieErr(err)
	}
	if !viper.IsSet(key) || len(accessKeyId) > 0 {
		viper.Set(key, accessKeyId)
	}
}
