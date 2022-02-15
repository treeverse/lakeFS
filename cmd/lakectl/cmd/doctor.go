package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
)

// doctorCmd represents the doctor command
var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run a basic diagnosis of the LakeFS configuration",
	Run: func(cmd *cobra.Command, args []string) {
		err := ListRepositoriesAndAnalyze(cmd.Context())
		if err == nil {
			fmt.Println("LakeFS doctor could not find any configuration issues")
			return
		}
		configFileName := viper.GetViper().ConfigFileUsed()
		fmt.Println("It looks like you have a problem with your `" + configFileName + "` file.")
		switch err.(type) {
		case *CredentialError:
			fmt.Println("It is possible that the `access_key_id` or `secret_access_key` you supplied are wrong.")
		case *WrongURIError:
			fmt.Println("Probably your endpoint url is wrong.")
		}
		errMsg := err.Error()
		if errMsg != "" {
			fmt.Println(errMsg)
		}
		accessKeyID := cfg.Values.Credentials.AccessKeyID
		if !IsValidAccessKeyID(accessKeyID) {
			fmt.Println("access_key_id value looks suspicious: accessKeyID")
		}

		secretAccessKey := cfg.Values.Credentials.SecretAccessKey
		if !IsValidSecretAccessKey(secretAccessKey) {
			fmt.Println("secret_access_key value looks suspicious...")
		}

		serverEndpoint := cfg.Values.Server.EndpointURL
		if !IsValidEndpointURI(serverEndpoint) {
			fmt.Println("Suspicious URI format for server.endpoint_url:", serverEndpoint)
		}
	},
}

func ListRepositoriesAndAnalyze(ctx context.Context) error {
	client := getClient()
	resp, err := client.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})

	if err != nil {
		urlErr := new(url.Error)
		if errors.As(err, &urlErr) {
			return new(WrongURIError)
		} else {
			return errors.New(err.Error())
		}
	} else {
		if resp != nil {
			if resp.JSON200 != nil {
				return nil
			}
			if resp.JSON401 != nil {
				return CredentialError{resp.JSON401.Message}
			} else {
				// In case we get the "not found" HTML page (the status is 200 and not 404 in this case)
				if resp.HTTPResponse != nil && resp.HTTPResponse.StatusCode == 200 {
					return new(WrongURIError)
				} else {
					if resp.JSONDefault != nil {
						return errors.New(resp.JSONDefault.Message)
					} else {
						return errors.New("")
					}
				}
			}
		}
	}
	return errors.New("")
}

type CredentialError struct {
	Message string
}

func (e CredentialError) Error() string {
	return e.Message
}

type WrongURIError struct {
	Message string
}

func (e WrongURIError) Error() string {
	return e.Message
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(doctorCmd)
}
