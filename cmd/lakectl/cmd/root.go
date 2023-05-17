package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/go-openapi/swag"
	"github.com/mitchellh/go-homedir"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/config"
	"github.com/treeverse/lakefs/pkg/api"
	config_types "github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	DefaultMaxIdleConnsPerHost = 100
	// version templates
	getLakeFSVersionErrorTemplate = `{{ print "Failed getting lakeFS server version:" | red }} {{ . }}
`
	getLatestVersionErrorTemplate = `{{ print "Failed getting latest lakectl version:" | red }} {{ . }}
`
	versionTemplate = `lakectl version: {{.LakectlVersion }}
{{- if .LakeFSVersion }}{{ "\n" }}lakeFS version: {{.LakeFSVersion}}{{ "\n" }}{{ end -}}
{{- if .UpgradeURL }}{{ "\n" }}{{ end -}}
{{- if .LakectlLatestVersion }}{{ print "lakectl out of date!"| yellow }} (Available: {{ .LakectlLatestVersion }}){{ "\n" }}{{ end -}}
{{- if .LakeFSLatestVersion }}{{ print "lakeFS out of date!"| yellow }} (Available: {{ .LakeFSLatestVersion }}){{ "\n" }}{{ end -}}
{{- if .UpgradeURL }}Get the latest release {{ .UpgradeURL|blue }}{{ "\n" }}{{ end -}}`
)

type versionInfo struct {
	LakectlVersion       string
	LakeFSVersion        string
	LakectlLatestVersion string
	LakeFSLatestVersion  string
	UpgradeURL           string
}

var (
	cfgFile string
	cfg     *config.Config

	// baseURI default value is set by the environment variable LAKECTL_BASE_URI and
	// override by flag 'base-url'. The baseURI is used as a prefix when we parse lakefs address (repo, ref or path).
	// The prefix is used only when the address we parse is not a full address (starts with 'lakefs://' scheme).
	// Examples:
	//   `--base-uri lakefs:// repo1` will resolve to repository `lakefs://repo1`
	//   `--base-uri lakefs://repo1 /main/file.md` will resolve to path `lakefs://repo1/main/file.md`
	baseURI string

	// logLevel logging level (default is off)
	logLevel string
	// logFormat logging format
	logFormat string
	// logOutputs logging outputs
	logOutputs []string

	// userAgentPrefix is the prefix to be used for the user agent value
	// of requests performed by the CLI.
	userAgentPrefix string
)

// rootCmd represents the base command when called without any sub-commands
var rootCmd = &cobra.Command{
	Use:   "lakectl",
	Short: "A cli tool to explore manage and work with lakeFS",
	Long:  `lakectl is a CLI tool allowing exploration and manipulation of a lakeFS environment`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		logging.SetLevel(logLevel)
		logging.SetOutputFormat(logFormat)
		logging.SetOutputs(logOutputs, 0, 0)
		if noColorRequested {
			DisableColors()
		}
		if cmd == configCmd {
			return
		}

		if cfg.Err() == nil {
			logging.Default().
				WithField("file", viper.ConfigFileUsed()).
				Debug("loaded configuration from file")
		}

		if errors.As(cfg.Err(), &viper.ConfigFileNotFoundError{}) {
			if cfgFile != "" {
				// specific message in case the file isn't found
				DieFmt("config file not found, please run \"lakectl config\" to create one\n%s\n", cfg.Err())
			}
			// if the config file wasn't provided, try to run using the default values + env vars
		} else if cfg.Err() != nil {
			// other errors while reading the config file
			DieFmt("error reading configuration file: %v", cfg.Err())
		}

		err := viper.UnmarshalExact(&cfg.Values, viper.DecodeHook(
			mapstructure.ComposeDecodeHookFunc(
				config_types.DecodeOnlyString,
				mapstructure.StringToTimeDurationHookFunc())))
		if err != nil {
			DieFmt("error unmarshal configuration: %v", err)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {

		if !MustBool(cmd.Flags().GetBool("version")) {
			if err := cmd.Help(); err != nil {
				WriteIfVerbose("failed showing help {{ . }}", err)
			}
			return
		}

		info := versionInfo{LakectlVersion: version.Version}

		// get lakeFS server version

		client := getClient()

		resp, err := client.GetLakeFSVersionWithResponse(cmd.Context())
		if err != nil {
			WriteIfVerbose(getLakeFSVersionErrorTemplate, err)
		} else if resp.JSON200 == nil {
			WriteIfVerbose(getLakeFSVersionErrorTemplate, resp.Status())
		} else {
			lakefsVersion := resp.JSON200
			info.LakeFSVersion = swag.StringValue(lakefsVersion.Version)
			if swag.BoolValue(lakefsVersion.UpgradeRecommended) {
				info.LakeFSLatestVersion = swag.StringValue(lakefsVersion.LatestVersion)
			}
			if swag.StringValue(lakefsVersion.UpgradeUrl) != "" {
				info.UpgradeURL = swag.StringValue(lakefsVersion.UpgradeUrl)
			}
		}
		// get lakectl latest version
		ghReleases := version.NewGithubReleases(version.GithubRepoOwner, version.GithubRepoName)
		latestVer, err := ghReleases.FetchLatestVersion()
		if err != nil {
			WriteIfVerbose(getLatestVersionErrorTemplate, err)
		} else {
			latest, err := version.CheckLatestVersion(latestVer)
			if err != nil {
				WriteIfVerbose("failed parsing {{ . }}", err)
			} else if latest.Outdated {
				info.LakectlLatestVersion = latest.LatestVersion
				if info.UpgradeURL == "" {
					info.UpgradeURL = version.DefaultReleasesURL
				}
			}
		}

		Write(versionTemplate, info)
	},
}

func getClient() *api.ClientWithResponses {
	// override MaxIdleConnsPerHost to allow highly concurrent access to our API client.
	// This is done to avoid accumulating many sockets in `TIME_WAIT` status that were closed
	// only to be immediately reopened.
	// see: https://stackoverflow.com/a/39834253
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	httpClient := &http.Client{
		Transport: transport,
	}

	accessKeyID := cfg.Values.Credentials.AccessKeyID
	secretAccessKey := cfg.Values.Credentials.SecretAccessKey
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(accessKeyID, secretAccessKey)
	if err != nil {
		DieErr(err)
	}

	serverEndpoint := cfg.Values.Server.EndpointURL
	u, err := url.Parse(serverEndpoint)
	if err != nil {
		DieErr(err)
	}
	// if no uri to api is set in configuration - set the default
	if u.Path == "" || u.Path == "/" {
		serverEndpoint = strings.TrimRight(serverEndpoint, "/") + api.BaseURL
	}

	client, err := api.NewClientWithResponses(
		serverEndpoint,
		api.WithHTTPClient(httpClient),
		api.WithRequestEditorFn(basicAuthProvider.Intercept),
		api.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			userAgent := "lakectl/" + version.Version
			if userAgentPrefix != "" {
				userAgent = userAgentPrefix + "_" + userAgent
			}
			req.Header.Set("User-Agent", userAgent)
			return nil
		}),
	)
	if err != nil {
		Die(fmt.Sprintf("could not initialize API client: %s", err), 1)
	}
	return client
}

// isSeekable returns true if f.Seek appears to work.
func isSeekable(f io.Seeker) bool {
	_, err := f.Seek(0, io.SeekCurrent)
	return err == nil // a little naive, but probably good enough for its purpose
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		DieErr(err)
	}
}

//nolint:gochecknoinits
func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.lakectl.yaml)")
	rootCmd.PersistentFlags().BoolVar(&noColorRequested, "no-color", false, "don't use fancy output colors (default when not attached to an interactive terminal)")
	rootCmd.PersistentFlags().StringVarP(&baseURI, "base-uri", "", os.Getenv("LAKECTL_BASE_URI"), "base URI used for lakeFS address parse")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "", "none", "set logging level")
	rootCmd.PersistentFlags().StringVarP(&logFormat, "log-format", "", "", "set logging output format")
	rootCmd.PersistentFlags().StringSliceVarP(&logOutputs, "log-output", "", []string{}, "set logging output(s)")
	rootCmd.PersistentFlags().StringVarP(&userAgentPrefix, "ua-prefix", "", os.Getenv("LAKECTL_UA_PREFIX"), "set user-agent prefix")
	rootCmd.PersistentFlags().BoolVar(&verboseMode, "verbose", false, "run in verbose mode")
	rootCmd.Flags().BoolP("version", "v", false, "version for lakectl")
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
			DieErr(err)
		}

		// Search config in home directory with name ".lakefs" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".lakectl")
	}

	viper.SetEnvPrefix("LAKECTL")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // support nested config
	viper.AutomaticEnv()                                   // read in environment variables that match

	cfg = config.ReadConfig()
}
