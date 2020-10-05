package stats

import (
	"cloud.google.com/go/compute/metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block/gs"
	s3a "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const (
	BlockstoreTypeKey = "blockstore_type"
	CloudAccountID    = "cloud_account_id"
)

type MetadataEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Metadata struct {
	InstallationID string          `json:"installation_id"`
	Entries        []MetadataEntry `json:"entries"`
}

func NewMetadata(logger logging.Logger, c *config.Config, db db.Database) *Metadata {
	res := &Metadata{}
	dbMetadataManager := auth.NewDBMetadataManager(config.Version, db)
	dbMetadata, err := dbMetadataManager.Write()
	if err != nil {
		logger.WithError(err).Debug("failed to collect account metadata")
	}
	for k, v := range dbMetadata {
		if k == auth.InstallationIDKeyName {
			res.InstallationID = v
		}
		res.Entries = append(res.Entries, MetadataEntry{Name: k, Value: v})
	}
	blockstoreType := c.GetBlockstoreType()
	res.Entries = append(res.Entries, MetadataEntry{Name: BlockstoreTypeKey, Value: blockstoreType})
	var accountID string
	switch blockstoreType {
	case s3a.BlockstoreType:
		accountID = getAWSAccountID(logger, c)
	case gs.BlockstoreType:
		accountID = getGoogleNumericProjectID(logger)
	}
	if accountID != "" {
		res.Entries = append(res.Entries, MetadataEntry{Name: CloudAccountID, Value: accountID})
	}
	return res
}

func getAWSAccountID(logger logging.Logger, c *config.Config) string {
	awsConfig := c.GetAwsConfig()
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		logger.Errorf("%v: failed to create AWS session for BI", err)
		return ""
	}
	sess.ClientConfig(s3.ServiceName)
	stsClient := sts.New(sess)
	identity, err := stsClient.GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		logger.Errorf("%v: failed to get AWS account ID for BI", err)
		return ""
	}
	return *identity.Account
}

func getGoogleNumericProjectID(logger logging.Logger) string {
	projectID, err := metadata.NumericProjectID()
	if err != nil {
		logger.Errorf("%v: failed to get Google numeric project ID from instance metadata", err)
		return ""
	}
	return projectID
}
