package cmd

import (
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/metastore"
	metastoreglue "github.com/treeverse/lakefs/pkg/metastore/glue"
	"github.com/treeverse/lakefs/pkg/metastore/hive"
	"golang.org/x/net/context"
)

var metastoreCmd = &cobra.Command{
	Use:   "metastore",
	Short: "Manage metastore commands",
}

func getGlueClient(c *Configuration) *glue.Client {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(string(c.Metastore.Glue.Region)),
		config.WithSharedCredentialsFiles([]string{string(c.Metastore.Glue.CredentialsFile)}),
		config.WithSharedConfigProfile(string(c.Metastore.Glue.Profile)),
	)
	if err != nil {
		return nil
	}

	client := glue.NewFromConfig(cfg, func(options *glue.Options) {
		if c.Metastore.Glue.Credentials != nil {
			options.Credentials = credentials.NewStaticCredentialsProvider(
				string(c.Metastore.Glue.Credentials.AccessKeyID),
				string(c.Metastore.Glue.Credentials.AccessSecretKey),
				string(c.Metastore.Glue.Credentials.SessionToken),
			)
		}
	})
	return client
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
		glueClient := getGlueClient(cfg)
		client, err := metastoreglue.NewMSClient(glueClient, cfg.Metastore.Glue.CatalogID.String(), cfg.Metastore.Glue.DBLocationURI.String())
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
