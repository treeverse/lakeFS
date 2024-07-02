package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/go-openapi/swag"
	"github.com/mitchellh/go-homedir"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	lakefsconfig "github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/git"
	"github.com/treeverse/lakefs/pkg/local"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/osinfo"
	"github.com/treeverse/lakefs/pkg/uri"
	"github.com/treeverse/lakefs/pkg/version"
	"golang.org/x/exp/slices"
)

const (
	DefaultMaxIdleConnsPerHost = 100
	// version templates
	getLakeFSVersionErrorTemplate = `{{ "Failed getting lakeFS server version:" | red }} {{ . }}
`
	getLatestVersionErrorTemplate = `{{ "Failed getting latest lakectl version:" | red }} {{ . }}
`
	versionTemplate = `lakectl version: {{.LakectlVersion }}
{{- if .LakeFSVersion }}
lakeFS version: {{.LakeFSVersion}}
{{- end }}
{{- if .UpgradeURL }}{{ "\n" }}{{ end -}}
{{- if .LakectlLatestVersion }}
{{ "lakectl out of date!" | yellow }} (Available: {{ .LakectlLatestVersion }})
{{- end }}
{{- if .LakeFSLatestVersion }}
{{ "lakeFS out of date!" | yellow }} (Available: {{ .LakeFSLatestVersion }})
{{- end }}
{{- if .UpgradeURL }}
Get the latest release {{ .UpgradeURL|blue }}
{{- end }}
`
)

type RetriesCfg struct {
	Enabled         bool          `mapstructure:"enabled"`
	MaxAttempts     int           `mapstructure:"max_attempts"`      // MaxAttempts is the maximum number of attempts
	MinWaitInterval time.Duration `mapstructure:"min_wait_interval"` // MinWaitInterval is the minimum amount of time to wait between retries
	MaxWaitInterval time.Duration `mapstructure:"max_wait_interval"` // MaxWaitInterval is the maximum amount of time to wait between retries
}

// Configuration is the user-visible configuration structure in Golang form.
// When editing, make sure *all* fields have a `mapstructure:"..."` tag, to simplify future refactoring.
type Configuration struct {
	Credentials struct {
		AccessKeyID     lakefsconfig.OnlyString `mapstructure:"access_key_id"`
		SecretAccessKey lakefsconfig.OnlyString `mapstructure:"secret_access_key"`
	} `mapstructure:"credentials"`
	Server struct {
		EndpointURL lakefsconfig.OnlyString `mapstructure:"endpoint_url"`
		Retries     RetriesCfg              `mapstructure:"retries"`
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
	// Experimental - Use caution when enabling experimental features. It should only be used after consulting with the lakeFS team!
	Experimental struct {
		Local struct {
			POSIXPerm struct {
				Enabled bool `mapstructure:"enabled"`
			} `mapstructure:"posix_permissions"`
		} `mapstructure:"local"`
	} `mapstructure:"experimental"`
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

const (
	recursiveFlagName   = "recursive"
	recursiveFlagShort  = "r"
	presignFlagName     = "pre-sign"
	parallelismFlagName = "parallelism"

	defaultSyncParallelism = 25
	defaultSyncPresign     = true

	myRepoExample   = "lakefs://my-repo"
	myBucketExample = "s3://my-bucket"
	myBranchExample = "my-branch"
	myRunIDExample  = "20230719152411arS0z6I"
	myDigestExample = "600dc0ffee"

	commitMsgFlagName     = "message"
	allowEmptyMsgFlagName = "allow-empty-message"
	fmtErrEmptyMsg        = `commit with no message without specifying the "--allow-empty-message" flag`
	metaFlagName          = "meta"

	defaultMaxAttempts      = 4
	defaultMaxRetryInterval = 30 * time.Second
	defaultMinRetryInterval = 200 * time.Millisecond
)

func withRecursiveFlag(cmd *cobra.Command, usage string) {
	cmd.Flags().BoolP(recursiveFlagName, recursiveFlagShort, false, usage)
}

func withParallelismFlag(cmd *cobra.Command) {
	cmd.Flags().IntP(parallelismFlagName, "p", defaultSyncParallelism,
		"Max concurrent operations to perform")
}

func withPresignFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(presignFlagName, defaultSyncPresign,
		"Use pre-signed URLs when downloading/uploading data (recommended)")
}

func withSyncFlags(cmd *cobra.Command) {
	withParallelismFlag(cmd)
	withPresignFlag(cmd)
}

type PresignMode struct {
	Enabled   bool
	Multipart bool
}

func getServerPreSignMode(ctx context.Context, client *apigen.ClientWithResponses) PresignMode {
	resp, err := client.GetConfigWithResponse(ctx)
	DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
	if resp.JSON200 == nil {
		Die("Bad response from server", 1)
	}
	storageConfig := resp.JSON200.StorageConfig
	return PresignMode{
		Enabled:   storageConfig.PreSignSupport,
		Multipart: swag.BoolValue(storageConfig.PreSignMultipartUpload),
	}
}

func getPresignMode(cmd *cobra.Command, client *apigen.ClientWithResponses) PresignMode {
	// use flags if set
	presignFlag := cmd.Flags().Lookup(presignFlagName)
	var presignMode PresignMode
	if presignFlag.Changed {
		presignMode.Enabled = Must(cmd.Flags().GetBool(presignFlagName))
	}
	// fetch server config if needed
	// if presign flag is not set, use server config
	// if presign flag is set, check if server supports multipart upload
	if !presignFlag.Changed || presignMode.Enabled {
		presignMode = getServerPreSignMode(cmd.Context(), client)
	}
	return presignMode
}

func getSyncFlags(cmd *cobra.Command, client *apigen.ClientWithResponses) local.SyncFlags {
	parallelism := Must(cmd.Flags().GetInt(parallelismFlagName))
	if parallelism < 1 {
		DieFmt("Invalid value for parallelism (%d), minimum is 1.\n", parallelism)
	}

	presignMode := getPresignMode(cmd, client)
	return local.SyncFlags{
		Parallelism:      parallelism,
		Presign:          presignMode.Enabled,
		PresignMultipart: presignMode.Multipart,
	}
}

// getSyncArgs parses arguments to extract a remote URI and deduces the local path.
// If the local path isn't provided and considerGitRoot is true, it uses the git repository root.
func getSyncArgs(args []string, requireRemote bool, considerGitRoot bool) (remote *uri.URI, localPath string) {
	idx := 0
	if requireRemote {
		remote = MustParsePathURI("path URI", args[0])
		idx += 1
	}

	if len(args) > idx {
		expanded := Must(homedir.Expand(args[idx]))
		localPath = Must(filepath.Abs(expanded))
		return
	}

	localPath = Must(filepath.Abs("."))
	if considerGitRoot {
		gitRoot, err := git.GetRepositoryPath(localPath)
		if err == nil {
			localPath = gitRoot
		} else if !(errors.Is(err, git.ErrNotARepository) || errors.Is(err, git.ErrNoGit)) { // allow support in environments with no git
			DieErr(err)
		}
	}
	return
}

func withMessageFlags(cmd *cobra.Command, allowEmpty bool) {
	cmd.Flags().StringP(commitMsgFlagName, "m", "", "commit message")
	cmd.Flags().Bool(allowEmptyMsgFlagName, allowEmpty, "allow an empty commit message")
}

func withMetadataFlag(cmd *cobra.Command) {
	cmd.Flags().StringSlice(metaFlagName, []string{}, "key value pair in the form of key=value")
}

func withCommitFlags(cmd *cobra.Command, allowEmptyMessage bool) {
	withMessageFlags(cmd, allowEmptyMessage)
	withMetadataFlag(cmd)
}

func getCommitFlags(cmd *cobra.Command) (string, map[string]string) {
	message := Must(cmd.Flags().GetString(commitMsgFlagName))
	emptyMessageBool := Must(cmd.Flags().GetBool(allowEmptyMsgFlagName))
	if strings.TrimSpace(message) == "" && !emptyMessageBool {
		DieFmt(fmtErrEmptyMsg)
	}

	kvPairs, err := getKV(cmd, metaFlagName)
	if err != nil {
		DieErr(err)
	}

	return message, kvPairs
}

func getKV(cmd *cobra.Command, name string) (map[string]string, error) { //nolint:unparam
	kvList, err := cmd.Flags().GetStringSlice(name)
	if err != nil {
		return nil, err
	}

	kv := make(map[string]string)
	for _, pair := range kvList {
		key, value, found := strings.Cut(pair, "=")
		if !found {
			return nil, errInvalidKeyValueFormat
		}
		kv[key] = value
	}
	return kv, nil
}

// rootCmd represents the base command when called without any sub-commands
var rootCmd = &cobra.Command{
	Use:   "lakectl",
	Short: "A cli tool to explore manage and work with lakeFS",
	Long:  `lakectl is a CLI tool allowing exploration and manipulation of a lakeFS environment`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		preRunCmd(cmd)
		sendStats(cmd, "")
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

		resp, err := client.GetConfigWithResponse(cmd.Context())
		if err != nil {
			WriteIfVerbose(getLakeFSVersionErrorTemplate, err)
		} else if resp.JSON200 == nil {
			WriteIfVerbose(getLakeFSVersionErrorTemplate, resp.Status())
		} else {
			lakefsVersion := resp.JSON200
			info.LakeFSVersion = swag.StringValue(lakefsVersion.VersionConfig.Version)
			if swag.BoolValue(lakefsVersion.VersionConfig.UpgradeRecommended) {
				info.LakeFSLatestVersion = swag.StringValue(lakefsVersion.VersionConfig.LatestVersion)
			}
			upgradeURL := swag.StringValue(lakefsVersion.VersionConfig.UpgradeUrl)
			if upgradeURL != "" {
				info.UpgradeURL = upgradeURL
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
	"doctor",
	"config",
}

func preRunCmd(cmd *cobra.Command) {
	logging.SetLevel(logLevel)
	logging.SetOutputFormat(logFormat)
	err := logging.SetOutputs(logOutputs, 0, 0)
	if err != nil {
		DieFmt("Failed to setup logging: %s", err)
	}
	if noColorRequested {
		DisableColors()
	}
	if cmd == configCmd {
		return
	}

	if cfgFile != "" && cfgErr != nil {
		DieFmt("error reading configuration file: %v", cfgErr)
	}

	logging.ContextUnavailable().
		WithField("file", viper.ConfigFileUsed()).
		Debug("loaded configuration from file")
	err = viper.UnmarshalExact(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		lakefsconfig.DecodeOnlyString,
		mapstructure.StringToTimeDurationHookFunc())))
	if err != nil {
		DieFmt("error unmarshal configuration: %v", err)
	}
}

func sendStats(cmd *cobra.Command, cmdSuffix string) {
	if version.IsVersionUnreleased() || !cmd.HasParent() { // Don't send statistics for root command
		return
	}
	var cmdName string
	for curr := cmd; curr.HasParent(); curr = curr.Parent() {
		if cmdName != "" {
			cmdName = curr.Name() + "_" + cmdName
		} else {
			cmdName = curr.Name()
		}
	}
	if cmdSuffix != "" {
		cmdName = cmdName + "_" + cmdSuffix
	}
	if !slices.Contains(excludeStatsCmds, cmdName) { // Skip excluded commands
		resp, err := getClient().PostStatsEventsWithResponse(cmd.Context(), apigen.PostStatsEventsJSONRequestBody{
			Events: []apigen.StatsEvent{
				{
					Class: "lakectl",
					Name:  cmdName,
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
}

func getHTTPClient() *http.Client {
	// Override MaxIdleConnsPerHost to allow highly concurrent access to our API client.
	// This is done to avoid accumulating many sockets in `TIME_WAIT` status that were closed
	// only to be immediately reopened.
	// see: https://stackoverflow.com/a/39834253
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	if !cfg.Server.Retries.Enabled {
		return &http.Client{Transport: transport}
	}
	return NewRetryClient(cfg.Server.Retries, transport)
}

func getClient() *apigen.ClientWithResponses {
	httpClient := getHTTPClient()

	accessKeyID := cfg.Credentials.AccessKeyID
	secretAccessKey := cfg.Credentials.SecretAccessKey
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(string(accessKeyID), string(secretAccessKey))
	if err != nil {
		DieErr(err)
	}

	serverEndpoint, err := apiutil.NormalizeLakeFSEndpoint(cfg.Server.EndpointURL.String())
	if err != nil {
		DieErr(err)
	}

	oss := osinfo.GetOSInfo()
	client, err := apigen.NewClientWithResponses(
		serverEndpoint,
		apigen.WithHTTPClient(httpClient),
		apigen.WithRequestEditorFn(basicAuthProvider.Intercept),
		apigen.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			// This UA string structure is agreed upon
			// Please consider that when making changes
			req.Header.Set("User-Agent", fmt.Sprintf("lakectl/%s/%s/%s/%s", version.Version, oss.OS, oss.Version, oss.Platform))
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
	rootCmd.PersistentFlags().BoolVar(&noColorRequested, "no-color", getEnvNoColor(), "don't use fancy output colors (default value can be set by NO_COLOR environment variable)")
	rootCmd.PersistentFlags().StringVarP(&baseURI, "base-uri", "", os.Getenv("LAKECTL_BASE_URI"), "base URI used for lakeFS address parse")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "", "none", "set logging level")
	rootCmd.PersistentFlags().StringVarP(&logFormat, "log-format", "", "", "set logging output format")
	rootCmd.PersistentFlags().StringSliceVarP(&logOutputs, "log-output", "", []string{}, "set logging output(s)")
	rootCmd.PersistentFlags().BoolVar(&verboseMode, "verbose", false, "run in verbose mode")
	rootCmd.Flags().BoolP("version", "v", false, "version for lakectl")
}

func getEnvNoColor() bool {
	v := os.Getenv("NO_COLOR")
	return v != "" && v != "0"
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
	var conf Configuration
	keys := lakefsconfig.GetStructKeys(reflect.TypeOf(conf), "mapstructure", "squash")
	for _, key := range keys {
		viper.SetDefault(key, nil)
	}

	// set defaults
	viper.SetDefault("metastore.hive.db_location_uri", "file:/user/hive/warehouse/")
	viper.SetDefault("server.endpoint_url", "http://127.0.0.1:8000")
	viper.SetDefault("server.retries.enabled", true)
	viper.SetDefault("server.retries.max_attempts", defaultMaxAttempts)
	viper.SetDefault("server.retries.max_wait_interval", defaultMaxRetryInterval)
	viper.SetDefault("server.retries.min_wait_interval", defaultMinRetryInterval)
	viper.SetDefault("experimental.local.posix_permissions.enabled", false)

	cfgErr = viper.ReadInConfig()
}
