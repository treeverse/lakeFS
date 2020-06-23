package cmd

import (
	"fmt"
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
	BucketNameFlag = "bucket"
	KeepFlag       = "keep"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a loadtest on a lakeFS instance",
	Long:  `Run a loadtest on a lakeFS instance. It can either be on a running lakeFS instance, or you can choose to start a dedicated lakeFS server as part of the test.`,
	Run: func(cmd *cobra.Command, args []string) {
		bucketName, err := cmd.Flags().GetString(BucketNameFlag)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		duration, _ := cmd.Flags().GetDuration(DurationFlag)
		requestsPerSeq, _ := cmd.Flags().GetInt(FrequencyFlag)
		isKeep, _ := cmd.Flags().GetBool(KeepFlag)
		testConfig := loadtest.Config{
			FreqPerSecond:     requestsPerSeq,
			Duration:          duration,
			BucketNameForRepo: bucketName,
			KeepRepo:          isKeep,
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
	runCmd.Flags().StringP(BucketNameFlag, "r", "", "Bucket to create the test repo in")
	_ = runCmd.MarkFlagRequired(BucketNameFlag)
	runCmd.Flags().Bool(KeepFlag, false, "Do not delete repo at the end of the test")
	runCmd.Flags().IntP(FrequencyFlag, "f", 5, "Number of requests to send per second")
	runCmd.Flags().DurationP(DurationFlag, "d", 30*time.Second, "Duration of test")
}
