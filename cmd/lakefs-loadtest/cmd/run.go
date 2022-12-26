package cmd

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/loadtest"

	"github.com/spf13/cobra"
)

const (
	DurationFlag         = "duration"
	FrequencyFlag        = "freq"
	RepoNameFlag         = "repo"
	StorageNamespaceFlag = "storage-namespace"
	KeepFlag             = "keep"
	MaxWorkersFlag       = "max-workers"

	defaultTestDuration = 30 * time.Second
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a loadtest on a lakeFS instance",
	Long:  `Run a loadtest on a lakeFS instance. It can either be on a running lakeFS instance, or you can choose to start a dedicated lakeFS server as part of the test.`,
	Run: func(cmd *cobra.Command, args []string) {
		repoName, err := cmd.Flags().GetString(RepoNameFlag)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		storageNamespace, err := cmd.Flags().GetString(StorageNamespaceFlag)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if (repoName == "" && storageNamespace == "") || (repoName != "" && storageNamespace != "") {
			fmt.Println(fmt.Sprintf("Exactly one of the following flags should be specified: "+
				"%s (to test on a new repo) or %s (to test on an existing repo)",
				StorageNamespaceFlag, RepoNameFlag))
			os.Exit(1)
		}
		duration, _ := cmd.Flags().GetDuration(DurationFlag)
		maxWorkers, _ := cmd.Flags().GetUint64(MaxWorkersFlag)
		requestsPerSeq, _ := cmd.Flags().GetInt(FrequencyFlag)
		isKeep, _ := cmd.Flags().GetBool(KeepFlag)
		testConfig := loadtest.Config{
			FreqPerSecond:    requestsPerSeq,
			Duration:         duration,
			MaxWorkers:       maxWorkers,
			RepoName:         repoName,
			StorageNamespace: storageNamespace,
			KeepRepo:         isKeep,
			Credentials: model.Credential{
				BaseCredential: model.BaseCredential{
					AccessKeyID:     viper.GetString(ConfigAccessKeyID),
					SecretAccessKey: viper.GetString(ConfigSecretAccessKey),
				},
			},
			ServerAddress: viper.GetString(ConfigServerEndpointURL),
			ShowProgress:  true,
		}
		loader := loadtest.NewLoader(testConfig)
		err = loader.Run()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

//nolint:gochecknoinits,gomnd
func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().StringP(RepoNameFlag, "r", "", "Existing lakeFS repo name to use. Leave empty to create a dedicated repo")
	runCmd.Flags().StringP(StorageNamespaceFlag, "s", "", "Storage namespace where data for the new repository will be stored (e.g.: s3://example-bucket/)")
	runCmd.Flags().Bool(KeepFlag, false, "Do not delete repo at the end of the test")
	runCmd.Flags().IntP(FrequencyFlag, "f", 5, "Number of requests to send per second")
	runCmd.Flags().DurationP(DurationFlag, "d", defaultTestDuration, "Duration of test")
	runCmd.Flags().Uint64(MaxWorkersFlag, math.MaxInt64, "Max workers used in the test")
}
