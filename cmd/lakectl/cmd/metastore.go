package cmd

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/glue"
	"github.com/treeverse/lakefs/pkg/metastore/hive"
)

var metastoreCmd = &cobra.Command{
	Use:   "metastore",
	Short: "Manage metastore commands",
}

func getMetastoreAwsConfig(c *Configuration) *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(string(c.Metastore.Glue.Region)),
	}
	if c.Metastore.Glue.Profile != "" || c.Metastore.Glue.CredentialsFile != "" {
		cfg.Credentials = credentials.NewSharedCredentials(
			string(c.Metastore.Glue.CredentialsFile),
			string(c.Metastore.Glue.Profile),
		)
	}
	if c.Metastore.Glue.Credentials != nil {
		cfg.Credentials = credentials.NewStaticCredentials(
			string(c.Metastore.Glue.Credentials.AccessKeyID),
			string(c.Metastore.Glue.Credentials.AccessSecretKey),
			string(c.Metastore.Glue.Credentials.SessionToken),
		)
	}
	return cfg
}

func getMetastoreClient(msType, hiveAddress string) (metastore.Client, func()) {
	if msType == "" {
		msType = cfg.Metastore.Type.String()
	}
	switch msType {
	case "hive":
		if len(hiveAddress) == 0 {
			hiveAddress = cfg.Metastore.Hive.URI.String()
		}
		hiveClient, err := hive.NewMSClient(hiveAddress, false, cfg.Metastore.Hive.DBLocationURI.String())
		if err != nil {
			DieErr(err)
		}
		deferFunc := func() {
			err := hiveClient.Close()
			if err != nil {
				DieErr(err)
			}
		}
		return hiveClient, deferFunc

	case "glue":
		awsConfig := getMetastoreAwsConfig(cfg)
		client, err := glue.NewMSClient(awsConfig, cfg.Metastore.Glue.CatalogID.String(), cfg.Metastore.Glue.DBLocationURI.String())
		if err != nil {
			DieErr(err)
		}
		return client, func() {}

	default:
		Die("unknown type, expected hive or glue got: "+msType, 1)
	}
	return nil, nil
}

func getClients(fromClientType, toClientType, fromAddress, toAddress string) (metastore.Client, metastore.Client, func(), func()) {
	msType := cfg.Metastore.Type.String()
	if len(fromClientType) == 0 {
		fromClientType = msType
	}
	if len(toClientType) == 0 {
		toClientType = msType
	}
	fromClient, fromClientDeferFunc := getMetastoreClient(fromClientType, fromAddress)
	toClient, toClientDeferFunc := getMetastoreClient(toClientType, toAddress)

	return fromClient, toClient, fromClientDeferFunc, toClientDeferFunc
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(metastoreCmd)
}
