package testutil

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/rs/xid"
	"github.com/spf13/viper"
	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/setup"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/logging"
)

const defaultSetupTimeout = 5 * time.Minute

func SetupTestingEnv(name, storageNS string) (logging.Logger, *genclient.Lakefs, *s3.S3) {
	logger := logging.Default()

	viper.SetDefault("setup_lakefs", true)
	viper.SetDefault("setup_lakefs_timeout", defaultSetupTimeout)
	viper.SetDefault("endpoint_url", "http://localhost:8000")
	viper.SetDefault("s3_endpoint", "s3.local.lakefs.io:8000")
	viper.SetDefault("access_key_id", "")
	viper.SetDefault("secret_access_key", "")
	viper.SetDefault("storage_namespace", fmt.Sprintf("s3://%s/%s", storageNS, xid.New().String()))

	viper.AddConfigPath(".")
	viper.SetEnvPrefix(strings.ToUpper(name))
	viper.SetConfigName(strings.ToLower(name))
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil && !errors.As(err, &viper.ConfigFileNotFoundError{}) {
		logger.WithError(err).Fatal("Failed to read configuration")
	}

	ctx := context.Background()

	// initialize the env/repo
	logger = logging.Default()
	logger.WithField("settings", viper.AllSettings()).Info(fmt.Sprintf("Starting %s", name))

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
	client := genclient.New(r, strfmt.Default)
	if err := waitUntilLakeFSRunning(ctx, logger, client); err != nil {
		logger.WithError(err).Fatal("Waiting for lakeFS")
	}

	setupLakeFS := viper.GetBool("setup_lakefs")
	if setupLakeFS {
		// first setup of lakeFS
		adminUserName := name
		res, err := client.Setup.SetupLakeFS(&setup.SetupLakeFSParams{
			User: &models.Setup{
				Username: swag.String(adminUserName),
			},
			Context: ctx,
		})
		if err != nil {
			logger.WithError(err).Fatal("Failed to setup lakeFS")
		}
		logger.Info("Cluster setup successfully")
		viper.Set("access_key_id", res.Payload.AccessKeyID)
		viper.Set("secret_access_key", res.Payload.SecretAccessKey)
	}
	r.DefaultAuthentication = httptransport.BasicAuth(viper.GetString("access_key_id"), viper.GetString("secret_access_key"))

	s3Endpoint := viper.GetString("s3_endpoint")
	awsSession := session.Must(session.NewSession())
	svc := s3.New(awsSession,
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

	return logger, client, svc
}

const checkIteration = 5 * time.Second

func waitUntilLakeFSRunning(ctx context.Context, logger logging.Logger, cl *genclient.Lakefs) error {
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
		case <-time.After(checkIteration):
		}
	}
}