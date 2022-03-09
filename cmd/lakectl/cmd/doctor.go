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

type Detailed interface {
	GetDetails() string
}

type CredentialsError struct {
	Message string
	Details string
}

func (e *CredentialsError) Error() string { return e.Message }

func (e *CredentialsError) GetDetails() string {
	return e.Message + "\n" + e.Details
}

type WrongEndpointURIError struct {
	Message string
	Details string
}

func (e *WrongEndpointURIError) Error() string { return e.Message }

func (e *WrongEndpointURIError) GetDetails() string {
	return e.Message + "\n" + e.Details
}

type UnknownConfigError struct {
	Message string
	Details string
}

func (e *UnknownConfigError) Error() string { return e.Message }

func (e *UnknownConfigError) GetDetails() string {
	return e.Message + "\n" + e.Details
}

type UserMessage struct {
	Message string
}

var detailedErrorTemplate = `{{ .Message |red }}
{{  .Details  }}
`
var errorTemplate = `{{ .Message |red }}
`

var successMessageTemplate = `{{ .Message | green}}
`

var analyzingMessageTemplate = `{{ .Message }}
`

// doctorCmd represents the doctor command
var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run a basic diagnosis of the LakeFS configuration",
	Long:  "Checking correctness of access key ID, secret access key and endpoint URL",
	Run: func(cmd *cobra.Command, args []string) {
		verbose, _ := cmd.Flags().GetBool("verbose")
		err := ListRepositoriesAndAnalyze(cmd.Context(), verbose)
		if err == nil {
			Write(successMessageTemplate, &UserMessage{"Valid configuration"})
			return
		}

		if detailedErr, ok := err.(Detailed); ok {
			Write(detailedErrorTemplate, detailedErr)
		} else {
			Write(errorTemplate, err)
		}

		if verbose {
			Write(analyzingMessageTemplate, &UserMessage{"Trying to validate access key format."})
		}
		accessKeyID := cfg.Values.Credentials.AccessKeyID
		if !IsValidAccessKeyID(accessKeyID) {
			Write(analyzingMessageTemplate, &UserMessage{"access_key_id value looks suspicious: " + accessKeyID})
		} else if verbose {
			Write(analyzingMessageTemplate, &UserMessage{"Couldn't find a problem with access key format."})
		}

		if verbose {
			Write(analyzingMessageTemplate, &UserMessage{"Trying to validate secret access key format."})
		}
		secretAccessKey := cfg.Values.Credentials.SecretAccessKey
		if !IsValidSecretAccessKey(secretAccessKey) {
			Write(analyzingMessageTemplate, &UserMessage{"secret_access_key value looks suspicious..."})
		} else if verbose {
			Write(analyzingMessageTemplate, &UserMessage{"Couldn't find a problem with secret access key format."})
		}

		if verbose {
			Write(analyzingMessageTemplate, &UserMessage{"Trying to validate endpoint URL format."})
		}
		serverEndpoint := cfg.Values.Server.EndpointURL
		if !strings.HasSuffix(serverEndpoint, api.BaseURL) {
			Write(analyzingMessageTemplate, &UserMessage{"Suspicious URI format for server.endpoint_url: " + serverEndpoint})
		} else if verbose {
			Write(analyzingMessageTemplate, &UserMessage{"Couldn't find a problem with endpoint URL format."})
		}
	},
}

func ListRepositoriesAndAnalyze(ctx context.Context, verboseMode bool) error {
	configFileName := viper.GetViper().ConfigFileUsed()
	msgOnErrUnknownConfig := "It looks like you have a problem with your `" + configFileName + "` file."
	msgOnErrWrongEndpointURI := "It looks like endpoint url is wrong."
	msgOnErrCredential := "It seems like the `access_key_id` or `secret_access_key` you supplied are wrong."

	if verboseMode {
		Write(analyzingMessageTemplate, &UserMessage{"Trying to get endpoint URL and parse it as an URL format."})
	}
	// getClient might die on url.Parse error, so check it first.
	serverEndpoint := cfg.Values.Server.EndpointURL
	_, err := url.Parse(serverEndpoint)
	if err != nil {
		return &WrongEndpointURIError{msgOnErrWrongEndpointURI, err.Error()}
	}
	client := getClient()
	if verboseMode {
		Write(analyzingMessageTemplate, &UserMessage{"Trying to run a simple command using current configuration."})
	}
	resp, err := client.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})

	switch {
	case err != nil:
		if verboseMode {
			Write(analyzingMessageTemplate, &UserMessage{"Got error while trying to run a simple command.\nTrying to analyze error."})
		}
		urlErr := &url.Error{}
		if errors.As(err, &urlErr) {
			return &WrongEndpointURIError{msgOnErrWrongEndpointURI, err.Error()}
		}
		return &UnknownConfigError{msgOnErrUnknownConfig, err.Error()}
	case resp == nil:
		break
	case resp.JSON200 != nil:
		return nil
	case resp.JSON401 != nil:
		return &CredentialsError{msgOnErrCredential, resp.JSON401.Message}
	// In case we get the "not found" HTML page (the status is 200 and not 404 in this case).
	case resp.HTTPResponse != nil && resp.HTTPResponse.StatusCode == 302:
		return &WrongEndpointURIError{msgOnErrWrongEndpointURI, ""}
	case resp.JSONDefault != nil:
		return &UnknownConfigError{msgOnErrUnknownConfig, resp.JSONDefault.Message}
	}
	return &UnknownConfigError{msgOnErrUnknownConfig, "An unknown error accourd while trying to analyzing LakeCtl configuration."}
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(doctorCmd)
	doctorCmd.Flags().BoolP("verbose", "v", false, "verbose mode")
}
