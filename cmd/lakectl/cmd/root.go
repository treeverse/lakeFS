package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/mitchellh/go-homedir"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tcnksm/go-latest"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/config"
	"github.com/treeverse/lakefs/pkg/api"
	config_types "github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	DefaultMaxIdleConnsPerHost = 100
)

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
		var (
			errLakeFSVersion error
			lakeFSVersion    string
		)
		versionVal, err := cmd.Flags().GetBool("version")
		if err != nil {
			DieErr(err)
		}
		if versionVal {
			// get lakeFS server version
			client := getClient()
			resp, errLakeFSVersion := client.GetLakeFSVersionWithResponse(context.Background())
			if errLakeFSVersion != nil {
				// TODO(isan): handle error
			}
			if resp.StatusCode() != http.StatusOK {
				// TODO(isan): handle error
			}
			if resp.JSON200 == nil || resp.JSON200.Version == nil {
				//TODO(isan): handle error
			}
			lakeFSVersion = *resp.JSON200.Version

		}
	},
	// Version: version.Version,
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
			req.Header.Set("User-Agent", "lakectl/"+version.Version)
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
	rootCmd.PersistentFlags().BoolVar(&verboseMode, "verbose", false, "run in verbose mode")
	rootCmd.Flags().BoolP("version", "v", false, "print version and exit")
	// fmt.Printf("CFG? %v\n", cfg)
	// initConfig()
	// fmt.Println("11111")
	// clt := getClient()
	// fmt.Printf("CFG? %v\n", cfg)
	// fmt.Println("2222")

	cobra.AddTemplateFunc("latestVersion", func(localVersion string) string {
		initConfig()
		fmt.Println("11111")
		clt := getClient()
		fmt.Println("2222")
		if clt != nil {
			resp, err := clt.GetLakeFSVersionWithResponse(context.Background())
			if err != nil {
				fmt.Println("error getting lakeFS version: " + err.Error())
			} else if resp.JSON200 != nil {
				fmt.Printf("LAKEFS SERVER VERSION: %v\n", *resp.JSON200)
			} else {
				fmt.Printf("NOT 2xx: %v\n", *resp)
			}
		} else {
			fmt.Println("CLIENT LAKEFS NIL")
		}
		githubTag := &latest.GithubTag{
			Owner:      "treeverse",
			Repository: "lakeFS",
			TagFilterFunc: func(version string) bool {
				// version start with v is optional and followed by 3 numbers with digits between them.	e.g v0.1.2
				match, _ := regexp.MatchString(`/^(v*)(0|[1-9]+[0-9]*)\.(0|[1-9]+[0-9]*)\.(0|[1-9]+[0-9]*)$`, version)
				return match
			},
		}
		res, err := latest.Check(githubTag, "0.1.0")
		if res == nil {
			return "its null - err: " + err.Error()
		}
		if res.Outdated {
			return fmt.Sprintf("%s is not latest, you should upgrade to %s", localVersion, res.Current)
		}
		if res.New {
			return fmt.Sprintf("%s is newer than current on source %s", localVersion, res.Current)
		}
		return ""
	})
	rootCmd.SetVersionTemplate("CHECKL THIS OUT {{.Version}} {{ .Annotations }} LATEST RESPONSE: {{ latestVersion .Version}}")
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
