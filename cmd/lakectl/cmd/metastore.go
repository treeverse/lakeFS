package cmd

import (
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/glue"
	"github.com/treeverse/lakefs/pkg/metastore/hive"
)

var metastoreCmd = &cobra.Command{
	Use:   "metastore",
	Short: "Manage metastore commands",
}

func getMetastoreClient(msType, hiveAddress string) (metastore.Client, func()) {
	if msType == "" {
		msType = cfg.GetMetastoreType()
	}
	switch msType {
	case "hive":
		if len(hiveAddress) == 0 {
			hiveAddress = cfg.GetMetastoreHiveURI()
		}
		hiveClient, err := hive.NewMSClient(hiveAddress, false, cfg.GetHiveDBLocationURI())
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
		client, err := glue.NewMSClient(cfg.GetMetastoreAwsConfig(), cfg.GetMetastoreGlueCatalogID(), cfg.GetGlueDBLocationURI())
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
	msType := cfg.GetMetastoreType()
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
