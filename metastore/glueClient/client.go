package glueClient

import (
	"fmt"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/metastore"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type GlueMSClient struct {
	svc       glueiface.GlueAPI
	catalogID *string
}

const MaxParts = 2 // max possible 1000

func NewGlueMSClient() *GlueMSClient {
	return &GlueMSClient{}
}

func (g *GlueMSClient) Open(catalogId string) error {
	//sess := session.Must(session.NewSession()) TODO: use config!
	cfg := &aws.Config{
		Region: aws.String(viper.GetString("blockstore.s3.region")),
		//Logger: &config.LogrusAWSAdapter{},
	}
	if viper.IsSet("blockstore.s3.profile") || viper.IsSet("blockstore.s3.credentials_file") {
		cfg.Credentials = credentials.NewSharedCredentials(
			viper.GetString("blockstore.s3.credentials_file"),
			viper.GetString("blockstore.s3.profile"))
	}
	if viper.IsSet("blockstore.s3.credentials") {
		cfg.Credentials = credentials.NewStaticCredentials(
			viper.GetString("blockstore.s3.credentials.access_key_id"),
			viper.GetString("blockstore.s3.credentials.access_secret_key"),
			viper.GetString("blockstore.s3.credentials.session_token"))
	}

	sess := session.Must(session.NewSession(cfg))
	sess.ClientConfig("glue")

	g.svc = glue.New(sess)
	g.catalogID = aws.String(catalogId)
	return nil
}

func (g *GlueMSClient) GetTable(dbName string, tblName string) (*glue.TableData, error) {
	table, err := g.svc.GetTable(&glue.GetTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		Name:         aws.String(tblName),
	})

	if err != nil {
		return nil, err
	}
	return table.Table, nil
}

func getAsTableInput(t *glue.TableData) *glue.TableInput {
	return &glue.TableInput{
		Description:       t.Description,
		LastAccessTime:    t.LastAccessTime,
		LastAnalyzedTime:  t.LastAnalyzedTime,
		Name:              t.Name,
		Owner:             t.Owner,
		Parameters:        t.Parameters,
		PartitionKeys:     t.PartitionKeys,
		Retention:         t.Retention,
		StorageDescriptor: t.StorageDescriptor,
		TableType:         t.TableType,
		ViewExpandedText:  t.ViewExpandedText,
		ViewOriginalText:  t.ViewOriginalText,
	}
}

func (g *GlueMSClient) createTable(dbName string, tbl *glue.TableData) error {
	_, err := g.svc.CreateTable(&glue.CreateTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		TableInput:   getAsTableInput(tbl),
	})
	return err
}

func (g *GlueMSClient) updateTable(dbName string, tbl *glue.TableData) error {
	_, err := g.svc.UpdateTable(&glue.UpdateTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		TableInput:   getAsTableInput(tbl),
	})
	return err
}

func (g *GlueMSClient) getPartitions(dbName, tableName string, nextToken *string, maxParts int16) (*glue.GetPartitionsOutput, error) {
	return g.svc.GetPartitions(&glue.GetPartitionsInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		MaxResults:   aws.Int64(int64(maxParts)), // max possible 1000
		NextToken:    nextToken,                  // todo: change this or the others
		TableName:    aws.String(tableName),
	})
}

func (g *GlueMSClient) getAllPartitions(dbName, tableName string) ([]*glue.Partition, error) {
	var nextToken *string
	gotPartitions := MaxParts // max possible 1000
	var allPartitions []*glue.Partition
	for gotPartitions == MaxParts {
		getPartitionsOutput, err := g.getPartitions(dbName, tableName, nextToken, MaxParts)
		if err != nil {
			return nil, err
		}
		nextToken = getPartitionsOutput.NextToken
		partitions := getPartitionsOutput.Partitions
		gotPartitions = len(partitions)
		allPartitions = append(allPartitions, partitions...)
	}
	return allPartitions, nil
}

func (g *GlueMSClient) addPartitions(dbName, tableName string, partitions []*glue.Partition) error {
	var partitionList []*glue.PartitionInput
	for _, partition := range partitions {
		partitionList = append(partitionList, &glue.PartitionInput{
			LastAccessTime:    partition.LastAccessTime,
			LastAnalyzedTime:  partition.LastAnalyzedTime,
			Parameters:        partition.Parameters,
			StorageDescriptor: partition.StorageDescriptor,
			Values:            partition.Values,
		})
	}
	req, output := g.svc.BatchCreatePartitionRequest(&glue.BatchCreatePartitionInput{
		CatalogId:          g.catalogID,
		DatabaseName:       aws.String(dbName),
		PartitionInputList: partitionList,
		TableName:          aws.String(tableName),
	})
	if output.Errors != nil {
		//todo : remove table
		for _, glueErrors := range output.Errors {
			log.Error(glueErrors.ErrorDetail) //todo: check what this returns
		}
		return fmt.Errorf("failed adding partitions")
	}
	return req.Send()
}

func (g *GlueMSClient) alterPartitions(dbName, tableName string, partitions []*glue.Partition) error {
	// no batch alter partitions we will need to do it one by one
	for _, partition := range partitions {
		_, err := g.svc.UpdatePartition(&glue.UpdatePartitionInput{
			CatalogId:    g.catalogID,
			DatabaseName: aws.String(dbName),
			PartitionInput: &glue.PartitionInput{
				LastAccessTime:    partition.LastAccessTime, //TODO check this out... have no idea if this is the write value
				LastAnalyzedTime:  partition.LastAnalyzedTime,
				Parameters:        partition.Parameters,
				StorageDescriptor: partition.StorageDescriptor,
				Values:            partition.Values,
			},
			PartitionValueList: partition.Values,
			TableName:          aws.String(tableName),
		})

		if err != nil {
			return err
		}

	}
	return nil
}

func (g *GlueMSClient) removePartitions(dbName, tableName string, partitions []*glue.Partition) error {
	// no batch alter partitions we will need to do it one by one
	var partitionsToDelete []*glue.PartitionValueList
	for _, partition := range partitions {
		partitionsToDelete = append(partitionsToDelete, &glue.PartitionValueList{Values: partition.Values})
	}
	_, err := g.svc.BatchDeletePartition(&glue.BatchDeletePartitionInput{
		CatalogId:          g.catalogID,
		DatabaseName:       aws.String(dbName),
		PartitionsToDelete: partitionsToDelete,
		TableName:          aws.String(tableName),
	})
	if err != nil {
		return err
	}
	return nil
}

func (g *GlueMSClient) copyPartitions(fromDBName, fromTable, fromBranch, toDBName, toTable, toBranch string) error {
	var nextToken *string
	gotPartitions := MaxParts
	for gotPartitions == MaxParts {
		getPartitionsOutput, err := g.getPartitions(fromDBName, fromTable, nextToken, MaxParts)
		if err != nil {
			return err
		}
		nextToken = getPartitionsOutput.NextToken
		partitions := getPartitionsOutput.Partitions
		gotPartitions = len(partitions)
		for _, partition := range partitions {
			partition.StorageDescriptor.SetLocation(metastore.TransformLocation(aws.StringValue(partition.StorageDescriptor.Location), fromBranch, toBranch))
			partition.SetTableName(toTable)
			partition.StorageDescriptor.SerdeInfo.SetName(toTable)
			partition.SetDatabaseName(toDBName)
		}
		_ = getPartitionsOutput.SetPartitions(partitions)
		err = g.addPartitions(toDBName, toTable, partitions)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *GlueMSClient) copyTable(fromDBName, fromTable, fromBranch, toDBName, toTable, toBranch string) error {
	t, err := g.svc.GetTable(&glue.GetTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(fromDBName),
		Name:         aws.String(fromTable),
	})
	if err != nil {
		return err
	}
	table := t.Table
	table.SetDatabaseName(toDBName)
	table.SetName(toTable)
	table.StorageDescriptor.SerdeInfo.SetName(toTable) //todo double check this, maybe serde should be somehting else
	table.StorageDescriptor.SetLocation(metastore.TransformLocation(aws.StringValue(table.StorageDescriptor.Location), fromBranch, toBranch))
	t.SetTable(table)
	err = g.createTable(toDBName, table)
	if err != nil {
		return err
	}
	return nil
}

func (g *GlueMSClient) CopyOrMerge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch string) error {
	table, err := g.GetTable(toDB, toTable)
	if err != nil {
		return err
	}
	if table != nil {
		return g.Copy(fromDB, fromTable, fromBranch, toDB, toTable, toBranch)
	}
	return g.Merge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch)
}

func (g *GlueMSClient) Copy(fromDBName, fromTable, fromBranch, toDBName, toTable, toBranch string) error {

	err := g.copyTable(fromDBName, fromTable, fromBranch, toDBName, toTable, toBranch)
	if err != nil {
		return err
	}
	err = g.copyPartitions(fromDBName, fromTable, fromBranch, toDBName, toTable, toBranch)
	return err
}

func (g *GlueMSClient) Merge(FromDBName, fromTable, fromBranch, toDBName, toTable, toBranch string) error {

	table, err := g.GetTable(FromDBName, fromTable)
	if err != nil {
		return err
	}
	table.SetDatabaseName(toDBName)
	table.SetName(toTable)
	table.StorageDescriptor.SerdeInfo.SetName(toTable) //todo double check this, maybe serde should be somehting else
	table.StorageDescriptor.SetLocation(metastore.TransformLocation(aws.StringValue(table.StorageDescriptor.Location), fromBranch, toBranch))

	partitions, err := g.getAllPartitions(FromDBName, fromTable)
	if err != nil {
		return err
	}
	toPartitions, err := g.getAllPartitions(toDBName, toTable)
	if err != nil {
		return err
	}

	//addPartitions, removePartitions, alterPartitions := diffPartitions(partitions, toPartitions, toDBName, toTable, toBranch, fromBranch)
	partitionIter := NewPartitionIter(partitions)
	toPartitionIter := NewPartitionIter(toPartitions)
	var addPartitions, removePartitions, alterPartitions []*glue.Partition
	metastore.Diff(partitionIter, toPartitionIter, func(difference catalog.DifferenceType, iter metastore.ComparableIterator) {
		switch difference {
		case catalog.DifferenceTypeRemoved:
			removePartitions = append(removePartitions, iter.(*PartitionIter).getCurrent())
		case catalog.DifferenceTypeAdded:
			addPartitions = append(addPartitions, iter.(*PartitionIter).getCurrent())
		default:
			alterPartitions = append(alterPartitions, iter.(*PartitionIter).getCurrent())
		}
	})

	err = g.updateTable(toDBName, table)
	if err != nil {
		return err
	}
	if len(addPartitions) > 0 {
		err = g.addPartitions(toDBName, toTable, addPartitions)
		if err != nil {
			return err
		}
	}
	if len(alterPartitions) > 0 {
		err = g.alterPartitions(toDBName, toTable, alterPartitions)
		if err != nil {
			return err
		}
	}
	if len(removePartitions) > 0 {
		err = g.removePartitions(toDBName, toTable, removePartitions)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *GlueMSClient) Diff(fromDB, fromTable, toDB, toTable string) (*metastore.MetaDiff, error) {

	diffColumns, err := g.getColumnDiff(fromDB, fromTable, toDB, toTable)
	if err != nil {
		return nil, err
	}
	// compare partitions
	partitionDiff, err := g.getPartitionsDiff(fromDB, fromTable, toDB, toTable)
	if err != nil {
		return nil, err
	}
	return &metastore.MetaDiff{
		PartitionDiff: partitionDiff,
		ColumnsDiff:   diffColumns,
	}, nil
}

func (g *GlueMSClient) getPartitionsDiff(FromDBName string, fromTable string, toDBName string, toTable string) (catalog.Differences, error) {
	partitions, err := g.getAllPartitions(FromDBName, fromTable)
	if err != nil {
		return nil, err
	}
	toPartitions, err := g.getAllPartitions(toDBName, toTable)
	if err != nil {
		return nil, err
	}

	partitionIter := NewPartitionIter(partitions)
	toPartitionIter := NewPartitionIter(toPartitions)
	return metastore.GetDiff(partitionIter, toPartitionIter), nil
}

func (g *GlueMSClient) getColumnDiff(fromDB, fromTable, toDB, toTable string) (catalog.Differences, error) {
	tableFrom, err := g.GetTable(fromDB, fromTable)
	if err != nil {
		return nil, err
	}
	tableTo, err := g.GetTable(toDB, toTable)
	if err != nil {
		return nil, err
	}

	colsIter := NewColumnIter(tableFrom.StorageDescriptor.Columns)
	colsToIter := NewColumnIter(tableTo.StorageDescriptor.Columns)
	return metastore.GetDiff(colsIter, colsToIter), nil
}

func (g *GlueMSClient) CopyToSymlink(fromDB, fromTable, toDB, toTable, bucket string) error {
	orig, err := g.GetTable(fromDB, fromTable)
	if err != nil {
		return err
	}
	orig.StorageDescriptor.SetInputFormat("org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat") // todo move to variable
	orig.SetDatabaseName(toDB)
	orig.SetName(toTable)
	orig.StorageDescriptor.SerdeInfo.SetName(toTable)
	orig.StorageDescriptor.SetLocation(metastore.GetSymlinkLocation(aws.StringValue(orig.StorageDescriptor.Location), bucket))

	//now partitions
	partitions, err := g.getAllPartitions(fromDB, fromTable)
	if err != nil {
		return err
	}
	for _, partition := range partitions {
		partition.SetDatabaseName(toDB)
		partition.SetTableName(toTable)
		partition.StorageDescriptor.SerdeInfo.SetName(toTable)
		partition.StorageDescriptor.SetLocation(metastore.GetSymlinkLocation(aws.StringValue(partition.StorageDescriptor.Location), bucket))
	}
	err = g.addPartitions(toDB, toTable, partitions)
	return err
}

func (g *GlueMSClient) MergeToSymlink(fromDB, fromTable, toDB, toTable, bucket string) error {
	orig, err := g.GetTable(fromDB, fromTable)
	if err != nil {
		return err
	}
	orig.StorageDescriptor.SetInputFormat("org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat") // todo move to variable
	orig.SetDatabaseName(toDB)
	orig.SetName(toTable)
	orig.StorageDescriptor.SerdeInfo.SetName(toTable)
	orig.StorageDescriptor.SetLocation(metastore.GetSymlinkLocation(aws.StringValue(orig.StorageDescriptor.Location), bucket))

	err = g.createTable(toDB, orig)
	if err != nil {
		return err
	}
	panic("implement merge symlink partitions")
	////now partitions
	//partitions, err := g.getAllPartitions(fromDB, fromTable)
	//if err != nil {
	//	return err
	//}
	//for _, partition := range partitions {
	//	partition.SetDatabaseName(toDB)
	//	partition.SetTableName(toTable)
	//	partition.StorageDescriptor.SerdeInfo.SetName(toTable)
	//	partition.StorageDescriptor.SetLocation(metastore.GetSymlinkLocation(aws.StringValue(partition.StorageDescriptor.Location), bucket))
	//}
	//
	//err = g.addPartitions(toDB, toTable, partitions)
	return err
}
