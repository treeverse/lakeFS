package cmd

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
)

var (
	ErrCredential       = errors.New("credential error")
	ErrWrongEndpointURI = errors.New("wrong endpoint_uri error")
	ErrUnknownConfig	= errors.New("unknown configuratioin error")
)

// doctorCmd represents the doctor command
var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run a basic diagnosis of the LakeFS configuration",
	Run: func(cmd *cobra.Command, args []string) {
		err := ListRepositoriesAndAnalyze(cmd.Context())
		if err == nil {
			Fmt("LakeFS doctor could not find any configuration issues\n")
		}
		configFileName := viper.GetViper().ConfigFileUsed()
		Fmt("It looks like you have a problem with your `%v` file.\n", configFileName)
		switch {
		case errors.Is(err, ErrCredential):
			Fmt("It is possible that the `access_key_id` or `secret_access_key` you supplied are wrong.\n")
		case errors.Is(err, ErrWrongEndpointURI):
			Fmt("Probably your endpoint url is wrong.\n")
		}
		accessKeyID := cfg.Values.Credentials.AccessKeyID
		if !IsValidAccessKeyID(accessKeyID) {
			Fmt("access_key_id value looks suspicious: %v\n", accessKeyID)
		}

		secretAccessKey := cfg.Values.Credentials.SecretAccessKey
		if !IsValidSecretAccessKey(secretAccessKey) {
			Fmt("secret_access_key value looks suspicious...")
		}

		serverEndpoint := cfg.Values.Server.EndpointURL
		if !strings.HasSuffix(serverEndpoint, api.BaseURL) {
			Fmt("Suspicious URI format for server.endpoint_url: %v, doesn't have `%v` suffix.\n", serverEndpoint, api.BaseURL)
		}
	},
}

func ListRepositoriesAndAnalyze(ctx context.Context) error {
	serverEndpoint := cfg.Values.Server.EndpointURL
	_, err := url.Parse(serverEndpoint)
	if err != nil {
		Fmt("%v\n", err.Error())
		return ErrWrongEndpointURI
	}
	client := getClient()
	resp, err := client.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})

	if err != nil {
		urlErr := new(url.Error)
		if errors.As(err, &urlErr) {
			Fmt("%v\n", err.Error())
			return ErrWrongEndpointURI
		}
		return err
	}
	if resp != nil {
		if resp.JSON200 != nil {
			return nil
		}
		if resp.JSON401 != nil {
			Fmt(resp.JSON401.Message)
			return ErrCredential
		}
		// In case we get the "not found" HTML page (the status is 200 and not 404 in this case)
		if resp.HTTPResponse != nil && resp.HTTPResponse.StatusCode == 200 {
			return ErrWrongEndpointURI
		}
		if resp.JSONDefault != nil {
			Fmt(resp.JSONDefault.Message)
		}
	}
	return ErrUnknownConfig
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(doctorCmd)
}
