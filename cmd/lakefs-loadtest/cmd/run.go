package cmd

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/loadtest"

	"github.com/spf13/cobra"
)

const (
	DurationFlag   = "duration"
	FrequencyFlag  = "freq"
	RepoNameFlag   = "repo"
	KeepFlag       = "keep"
	MaxWorkersFlag = "max-workers"
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
		duration, _ := cmd.Flags().GetDuration(DurationFlag)
		requestsPerSeq, _ := cmd.Flags().GetInt(FrequencyFlag)
		isKeep, _ := cmd.Flags().GetBool(KeepFlag)
		maxWorkers, _ := cmd.Flags().GetUint64(MaxWorkersFlag)
		testConfig := loadtest.Config{
			FreqPerSecond: requestsPerSeq,
			Duration:      duration,
			MaxWorkers:    maxWorkers,
			RepoName:      repoName,
			KeepRepo:      isKeep,
			Credentials: model.Credential{
				AccessKeyId:     viper.GetString(ConfigAccessKeyId),
				AccessSecretKey: viper.GetString(ConfigSecretAccessKey),
			},
			ServerAddress: viper.GetString(ConfigServerEndpointUrl),
		}
		loader := loadtest.NewLoader(testConfig)
		err = loader.Run()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().StringP(RepoNameFlag, "r", "", "Existing lakeFS repo name to use. Leave empty to create a dedicated repo")
	runCmd.Flags().Bool(KeepFlag, false, "Do not delete repo at the end of the test")
	runCmd.Flags().IntP(FrequencyFlag, "f", 5, "Number of requests to send per second")
	runCmd.Flags().DurationP(DurationFlag, "d", 30*time.Second, "Duration of test")
	runCmd.Flags().Uint64(MaxWorkersFlag, math.MaxInt64, "Max workers used in the test")
}
