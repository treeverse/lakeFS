package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/go-openapi/swag"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
	lakefsconfig "github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
	"golang.org/x/exp/slices"
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

// Configuration is the user-visible configuration structure in Golang form.
// When editing, make sure *all* fields have a `mapstructure:"..."` tag, to simplify future refactoring.
type Configuration struct {
	Credentials struct {
		AccessKeyID     lakefsconfig.OnlyString `mapstructure:"access_key_id"`
		SecretAccessKey lakefsconfig.OnlyString `mapstructure:"secret_access_key"`
	} `mapstructure:"credentials"`
	Server struct {
		EndpointURL lakefsconfig.OnlyString `mapstructure:"endpoint_url"`
	} `mapstructure:"server"`
	Metastore struct {
		Type lakefsconfig.OnlyString `mapstructure:"type"`
		Hive struct {
			URI           lakefsconfig.OnlyString `mapstructure:"uri"`
			DBLocationURI lakefsconfig.OnlyString `mapstructure:"db_location_uri"`
		} `mapstructure:"hive"`
		Glue struct {
			// TODO(ariels): Refactor credentials to share with server side.
			Profile         lakefsconfig.OnlyString `mapstructure:"profile"`
			CredentialsFile lakefsconfig.OnlyString `mapstructure:"credentials_file"`
			DBLocationURI   lakefsconfig.OnlyString `mapstructure:"db_location_uri"`
			Credentials     *struct {
				AccessKeyID     lakefsconfig.OnlyString `mapstructure:"access_key_id"`
				AccessSecretKey lakefsconfig.OnlyString `mapstructure:"access_secret_key"`
				SessionToken    lakefsconfig.OnlyString `mapstructure:"session_token"`
			} `mapstructure:"credentials"`

			Region    lakefsconfig.OnlyString `mapstructure:"region"`
			CatalogID lakefsconfig.OnlyString `mapstructure:"catalog_id"`
		} `mapstructure:"glue"`
		// setting FixSparkPlaceholder to true will change spark placeholder with the actual location. for more information see https://github.com/treeverse/lakeFS/issues/2213
		FixSparkPlaceholder bool `mapstructure:"fix_spark_placeholder"`
	}
}

type versionInfo struct {
	LakectlVersion       string
	LakeFSVersion        string
	LakectlLatestVersion string
	LakeFSLatestVersion  string
	UpgradeURL           string
}

var (
	cfgFile string
	cfgErr  error
	cfg     *Configuration

	// baseURI default value is set by the environment variable LAKECTL_BASE_URI and
	// override by flag 'base-url'. The baseURI is used as a prefix when we parse lakefs address (repo, ref or path).
	// The prefix is used only when the address we parse is not a full address (starts with 'lakefs://' scheme).
	// Examples:
	//   `--base-uri lakefs:// repo1` will resolve to repository `lakefs://repo1`
	//   `--base-uri lakefs://repo1 /main/file.md` will resolve to path `lakefs://repo1/main/file.md`
	baseURI string

	// logLevel logging level (default is off)
	logLevel string
	// logFormat logging output format
	logFormat string
	// logOutputs logging outputs
	logOutputs []string

	// noColorRequested is set to true when the user requests no color output
	noColorRequested = false

	// verboseMode is set to true when the user requests verbose output
	verboseMode = false
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

		if cfgErr == nil {
			logging.ContextUnavailable().
				WithField("file", viper.ConfigFileUsed()).
				Debug("loaded configuration from file")
		} else if errors.As(cfgErr, &viper.ConfigFileNotFoundError{}) {
			if cfgFile != "" {
				// specific message in case the file isn't found
				DieFmt("config file not found, please run \"lakectl config\" to create one\n%s\n", cfgErr)
			}
			// if the config file wasn't provided, try to run using the default values + env vars
		} else if cfgErr != nil {
			// other errors while reading the config file
			DieFmt("error reading configuration file: %v", cfgErr)
		}

		err := viper.UnmarshalExact(&cfg, viper.DecodeHook(
			mapstructure.ComposeDecodeHookFunc(
				lakefsconfig.DecodeOnlyString,
				mapstructure.StringToTimeDurationHookFunc())))
		if err != nil {
			DieFmt("error unmarshal configuration: %v", err)
		}

		if cmd.HasParent() {
			// Don't send statistics for root command or if one of the excluding
			var cmdName string
			for curr := cmd; curr.HasParent(); curr = curr.Parent() {
				if cmdName != "" {
					cmdName = curr.Name() + "_" + cmdName
				} else {
					cmdName = curr.Name()
				}
			}
			if !slices.Contains(excludeStatsCmds, cmdName) {
				sendStats(cmd.Context(), getClient(), cmdName)
			}
		}

	},
	Run: func(cmd *cobra.Command, args []string) {
		if !Must(cmd.Flags().GetBool("version")) {
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

var excludeStatsCmds = []string{
	"lakectl_doctor",
	"lakectl_config",
}

func sendStats(ctx context.Context, client api.ClientWithResponsesInterface, cmd string) {
	resp, err := client.PostStatsEventsWithResponse(ctx, api.PostStatsEventsJSONRequestBody{
		Events: []api.StatsEvent{
			{
				Class: "lakectl",
				Name:  cmd,
				Count: 1,
			},
		},
	})

	var errStr string
	if err != nil {
		errStr = err.Error()
	} else if resp.StatusCode() != http.StatusNoContent {
		errStr = resp.Status()
	}
	if errStr != "" {
		_, _ = fmt.Fprintf(os.Stderr, "Warning: failed sending statistics: %s\n", errStr)
	}
}

func getClient() *api.ClientWithResponses {
	// Override MaxIdleConnsPerHost to allow highly concurrent access to our API client.
	// This is done to avoid accumulating many sockets in `TIME_WAIT` status that were closed
	// only to be immediately reopened.
	// see: https://stackoverflow.com/a/39834253
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	httpClient := &http.Client{
		Transport: transport,
	}

	accessKeyID := cfg.Credentials.AccessKeyID
	secretAccessKey := cfg.Credentials.SecretAccessKey
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(string(accessKeyID), string(secretAccessKey))
	if err != nil {
		DieErr(err)
	}

	serverEndpoint := cfg.Server.EndpointURL.String()
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
			req.Header.Set("User-Agent", "lakectl/"+version.Version)
			return nil
		}),
	)
	if err != nil {
		Die(fmt.Sprintf("could not initialize API client: %s", err), 1)
	}
	return client
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
		home, err := os.UserHomeDir()
		if err != nil {
			DieErr(err)
		}

		// Search config in home directory
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".lakectl")
	}
	viper.SetEnvPrefix("LAKECTL")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // support nested config
	viper.AutomaticEnv()                                   // read in environment variables that match

	// Inform viper of all expected fields.
	// Otherwise, it fails to deserialize from the environment.
	var cfg Configuration
	keys := lakefsconfig.GetStructKeys(reflect.TypeOf(cfg), "mapstructure", "squash")
	for _, key := range keys {
		viper.SetDefault(key, nil)
	}

	// set defaults
	viper.SetDefault("metastore.hive.db_location_uri", "file:/user/hive/warehouse/")
	viper.SetDefault("server.endpoint_url", "http://127.0.0.1:8000")

	cfgErr = viper.ReadInConfig()
	if errors.Is(cfgErr, viper.ConfigFileNotFoundError{}) {
		DieFmt("Failed to read config file '%s': %s", viper.ConfigFileUsed(), cfgErr)
	}
}
