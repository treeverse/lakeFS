package nessie

import (
	"context"
	"errors"
	"flag"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/spf13/viper"
	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/setup"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/logging"
)

var (
	logger logging.Logger
	client *genclient.Lakefs
	svc    *s3.S3
)

func TestMain(m *testing.M) {
	systemTests := flag.Bool("system-tests", false, "Run system tests")
	flag.Parse()
	if !*systemTests {
		os.Exit(0)
	}
	logger = logging.Default()

	viper.SetDefault("setup_lakefs", true)
	viper.SetDefault("setup_lakefs_timeout", 5*time.Minute)
	viper.SetDefault("endpoint_url", "http://localhost:8000")
	viper.SetDefault("s3_endpoint", "s3.local.lakefs.io:8000")
	viper.SetDefault("access_key_id", "")
	viper.SetDefault("secret_access_key", "")
	viper.SetDefault("storage_namespace", "s3://nessie-system-testing")

	viper.AddConfigPath(".")
	viper.SetEnvPrefix("NESSIE")
	viper.SetConfigName("nessie")
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil && !errors.As(err, &viper.ConfigFileNotFoundError{}) {
		logger.WithError(err).Fatal("Failed to read configuration")
	}

	ctx := context.Background()

	// initialize the env/repo
	logger = logging.Default()
	logger.WithField("settings", viper.AllSettings()).Info("Starting nessie")

	endpointURL := viper.GetString("endpoint_url")
	u, err := url.Parse(endpointURL)
	if err != nil {
		logger.WithError(err).Fatal("Failed to parse endpoint URL", endpointURL)
	}

	apiBasePath := genclient.DefaultBasePath
	if u.Path != "" {
		apiBasePath = u.Path
	}
	r := httptransport.New(u.Host, apiBasePath, []string{u.Scheme})
	client = genclient.New(r, strfmt.Default)
	if err := waitUntilLakeFSRunning(ctx, client); err != nil {
		logger.WithError(err).Fatal("Waiting for lakeFS")
	}

	setupLakeFS := viper.GetBool("setup_lakefs")
	if setupLakeFS {
		// first setup of lakeFS
		const adminUserName = "nessie"
		res, err := client.Setup.SetupLakeFS(&setup.SetupLakeFSParams{
			User: &models.Setup{
				DisplayName: swag.String(adminUserName),
			},
			Context: ctx,
		})
		if err != nil {
			logger.WithError(err).Fatal("Failed to setup lakeFS")
		}
		logger.Info("Cluster setup successfully")
		viper.Set("access_key_id", res.Payload.AccessKeyID)
		viper.Set("secret_access_key", res.Payload.AccessSecretKey)
	}
	r.DefaultAuthentication = httptransport.BasicAuth(viper.GetString("access_key_id"), viper.GetString("secret_access_key"))

	s3Endpoint := viper.GetString("s3_endpoint")
	awsSession := session.Must(session.NewSession())
	svc = s3.New(awsSession,
		aws.NewConfig().
			WithRegion("us-east-1").
			WithEndpoint(s3Endpoint).
			WithDisableSSL(true).
			WithCredentials(credentials.NewCredentials(
				&credentials.StaticProvider{
					Value: credentials.Value{
						AccessKeyID:     viper.GetString("access_key_id"),
						SecretAccessKey: viper.GetString("secret_access_key"),
					}})))

	logger.Info("Setup succeeded, running the tests")
	os.Exit(m.Run())
}

func waitUntilLakeFSRunning(ctx context.Context, cl *genclient.Lakefs) error {
	setupCtx, cancel := context.WithTimeout(ctx, viper.GetDuration("setup_lakefs_timeout"))
	defer cancel()
	for {
		_, err := cl.HealthCheck.HealthCheck(nil)
		if err == nil {
			return nil
		}
		logger.WithError(err).Info("Setup failed")

		select {
		case <-setupCtx.Done():
			return setupCtx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}
