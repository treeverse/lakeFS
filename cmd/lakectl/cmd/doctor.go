package cmd

import (
	"errors"
	"fmt"
	"strings"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/config"
	"github.com/treeverse/lakefs/pkg/api"
)

// doctorCmd represents the doctor command
var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run a basic diagnosis of the LakeFS configuration",
	Run: func(cmd *cobra.Command, args []string) {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			DieErr(err)
		}

		// Search config in home directory with name ".lakefs" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".lakectl")
		viper.SetEnvPrefix("LAKECTL")
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // support nested config
		viper.AutomaticEnv()                                   // read in environment variables that match

		cfg := config.ReadConfig()
		cfgError := cfg.Err()
		if cfgError != nil {
			if errors.As(cfgError, &viper.ConfigFileNotFoundError{}) {
				// specific message in case the file isn't found
				DieFmt("Config file not found, please run \"lakectl config\" to create one\n%s\n", cfg.Err())
			}
			// other errors while reading the config file
			DieFmt("Error reading configuration file: %v", cfgError)
		}

		if err := viper.UnmarshalExact(&cfg.Values); err != nil {
			DieFmt("Error unmarshal configuration: %v", err)
		}

		accessKeyID := cfg.Values.Credentials.AccessKeyID
		if !IsValidAccessKeyID(accessKeyID) {
			fmt.Println("access_key_id value looks suspicious...")
		}

		secretAccessKey := cfg.Values.Credentials.SecretAccessKey
		if !IsValidSecretAccessKey(secretAccessKey) {
			fmt.Println("secret_access_key value looks suspicious...")
		}

		basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(accessKeyID, secretAccessKey)
		if err != nil {
			DieErr(err)
		}

		serverEndpoint := cfg.Values.Server.EndpointURL
		if !IsValidEndpointURI(serverEndpoint) {
			DieFmt("Wrong URI format for server.endpoint_url: %v", serverEndpoint)
		}

		client, err := api.NewClientWithResponses(
			serverEndpoint,
			api.WithRequestEditorFn(basicAuthProvider.Intercept),
		)
		if err != nil {
			DieFmt("Could not initialize API client: %v", err.Error())
		}

		loginReq := api.LoginJSONRequestBody{
			AccessKeyId:     accessKeyID,
			SecretAccessKey: secretAccessKey,
		}

		login, err := client.LoginWithResponse(cmd.Context(), loginReq)
		if err != nil || login.JSON200 == nil {
			DieFmt("It looks like you have a problem with your '.lakectl.yaml' file. \nIt is possible that the access_key_id' or 'secret_access_key' you supplied are wrong \n\n")
		}

		rsp, _ := client.ListRepositoriesWithResponse(cmd.Context(), &api.ListRepositoriesParams{})
		rsp.StatusCode()
		if rsp.JSON200 != nil {
			fmt.Println("LakeFS doctor could not find any configuration issues")
		} else {
			fmt.Print("It looks like you have a problem with your '.lakectl.yaml' file. \nIt is possible that the access_key_id' or 'secret_access_key' you supplied are wrong \n\n")
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(doctorCmd)
}
