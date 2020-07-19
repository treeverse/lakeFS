package glue

import (
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/metastore"
)

type Client struct {
	*MSClient
	Client    glueiface.GlueAPI
	transport thrift.TTransport
}

func NewClient(cfg *aws.Config, catalogID string) (*Client, error) {
	c := &Client{}
	sess := session.Must(session.NewSession(cfg))
	sess.ClientConfig("glue")
	gl := glue.New(sess)

	c.MSClient = &MSClient{
		client:    gl,
		catalogID: aws.String(catalogID),
	}
	return c, nil
}

type MSClient struct {
	client    glueiface.GlueAPI
	catalogID *string /**/
}

const MaxParts = 1000 // max possible 1000

func GetGlueService(cfg *aws.Config) *glue.Glue {
	sess := session.Must(session.NewSession(cfg))
	sess.ClientConfig("glue")
	return glue.New(sess)
}

func NewGlueMSClient(client glueiface.GlueAPI, catalogID string) *MSClient {
	return &MSClient{
		client:    client,
		catalogID: aws.String(catalogID),
	}
}

func (g *MSClient) getTable(dbName string, tblName string) (*glue.TableData, error) {
	table, err := g.client.GetTable(&glue.GetTableInput{
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

func getAsPartitionInput(t *glue.Partition) *glue.PartitionInput {
	return &glue.PartitionInput{
		LastAccessTime:    t.LastAccessTime,
		LastAnalyzedTime:  t.LastAnalyzedTime,
		Parameters:        t.Parameters,
		StorageDescriptor: t.StorageDescriptor,
		Values:            t.Values,
	}
}

func (g *MSClient) createTable(dbName string, tbl *glue.TableData) error {
	_, err := g.client.CreateTable(&glue.CreateTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		TableInput:   getAsTableInput(tbl),
	})
	return err
}

func (g *MSClient) updateTable(dbName string, tbl *glue.TableData) error {
	_, err := g.client.UpdateTable(&glue.UpdateTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		TableInput:   getAsTableInput(tbl),
	})
	return err
}
func (g *MSClient) getPartition(dbName, tableName string, partitionValues []string) (*glue.Partition, error) {
	output, err := g.client.GetPartition(&glue.GetPartitionInput{
		CatalogId:       g.catalogID,
		DatabaseName:    aws.String(dbName),
		PartitionValues: aws.StringSlice(partitionValues),
		TableName:       aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}
	return output.Partition, nil
}

func (g *MSClient) getPartitions(dbName, tableName string, nextToken *string, maxParts int16) (*glue.GetPartitionsOutput, error) {
	return g.client.GetPartitions(&glue.GetPartitionsInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		MaxResults:   aws.Int64(int64(maxParts)),
		NextToken:    nextToken,
		TableName:    aws.String(tableName),
	})
}

func (g *MSClient) getAllPartitions(dbName, tableName string) ([]*glue.Partition, error) {
	var nextToken *string
	gotPartitions := MaxParts
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

func (g *MSClient) addPartition(dbName, tableName string, partition *glue.Partition) error {
	_, err := g.client.CreatePartition(&glue.CreatePartitionInput{
		CatalogId:      g.catalogID,
		DatabaseName:   aws.String(dbName),
		PartitionInput: getAsPartitionInput(partition),
		TableName:      aws.String(tableName),
	})
	return err
}

func (g *MSClient) addPartitions(dbName, tableName string, partitions []*glue.Partition) error {
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
	_, err := g.client.BatchCreatePartition(&glue.BatchCreatePartitionInput{
		CatalogId:          g.catalogID,
		DatabaseName:       aws.String(dbName),
		PartitionInputList: partitionList,
		TableName:          aws.String(tableName),
	})
	return err
}

func (g *MSClient) alterPartition(dbName, tableName string, partition *glue.Partition) error {
	// No batch alter partitions we will need to do it one by one

	_, err := g.client.UpdatePartition(&glue.UpdatePartitionInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		PartitionInput: &glue.PartitionInput{
			LastAccessTime:    partition.LastAccessTime,
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

	return nil
}

func (g *MSClient) alterPartitions(dbName, tableName string, partitions []*glue.Partition) error {
	// No batch alter partitions we will need to do it one by one
	for _, partition := range partitions {
		err := g.alterPartition(dbName, tableName, partition)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *MSClient) removePartitions(dbName, tableName string, partitions []*glue.Partition) error {
	// no batch alter partitions we will need to do it one by one
	var partitionsToDelete []*glue.PartitionValueList
	for _, partition := range partitions {
		partitionsToDelete = append(partitionsToDelete, &glue.PartitionValueList{Values: partition.Values})
	}
	_, err := g.client.BatchDeletePartition(&glue.BatchDeletePartitionInput{
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

func (g *MSClient) copyPartitions(fromDBName, fromTable, toDBName, toTable, serde string, symlink bool, transformLocation func(location string) string) error {
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
			if symlink {
				partition.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
			}

			partition.StorageDescriptor.SetLocation(transformLocation(aws.StringValue(partition.StorageDescriptor.Location)))
			partition.SetTableName(toTable)
			partition.StorageDescriptor.SerdeInfo.SetName(serde)
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

func (g *MSClient) copyTable(fromDB, fromTable, toDB, toTable, serde string, symlink bool, transformLocation func(location string) string) error {
	t, err := g.client.GetTable(&glue.GetTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(fromDB),
		Name:         aws.String(fromTable),
	})
	if err != nil {
		return err
	}
	table := t.Table
	table.SetDatabaseName(toDB)
	table.SetName(toTable)
	table.StorageDescriptor.SerdeInfo.SetName(serde)
	table.StorageDescriptor.SetLocation(transformLocation(aws.StringValue(table.StorageDescriptor.Location)))
	if symlink {
		table.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
		table.StorageDescriptor.SetLocation(transformLocation(""))
	}
	t.SetTable(table)
	err = g.createTable(toDB, table)
	if err != nil {
		return err
	}
	return nil
}

func (g *MSClient) copy(fromDB, fromTable, toDB, toTable, serde string, symlink bool, transformLocation func(location string) string) error {

	err := g.copyTable(fromDB, fromTable, toDB, toTable, serde, symlink, transformLocation)
	if err != nil {
		return err
	}
	err = g.copyPartitions(fromDB, fromTable, toDB, toTable, serde, symlink, transformLocation)
	return err
}

func (g *MSClient) merge(FromDB, fromTable, toDB, toTable, serde string, symlink bool, transformLocation func(location string) string) error {

	table, err := g.getTable(FromDB, fromTable)
	if err != nil {
		return err
	}
	table.SetDatabaseName(toDB)
	table.SetName(toTable)
	table.StorageDescriptor.SerdeInfo.SetName(serde)
	table.StorageDescriptor.SetLocation(transformLocation(aws.StringValue(table.StorageDescriptor.Location)))
	if symlink {
		table.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
	}
	partitions, err := g.getAllPartitions(FromDB, fromTable)
	if err != nil {
		return err
	}
	toPartitions, err := g.getAllPartitions(toDB, toTable)
	if err != nil {
		return err
	}

	partitionIter := NewPartitionIter(partitions)
	toPartitionIter := NewPartitionIter(toPartitions)
	var addPartitions, removePartitions, alterPartitions []*glue.Partition
	metastore.DiffIterable(partitionIter, toPartitionIter, func(difference catalog.DifferenceType, value interface{}, _ string) {
		partition, ok := value.(*glue.Partition)
		if !ok {
			msg := fmt.Sprintf("unexpected value in diffIterable call. expected to get  *glue.Partition, but got: %T", value)
			panic(msg)
		}
		partition.SetDatabaseName(toDB)
		partition.SetTableName(toTable)
		partition.StorageDescriptor.SetLocation(transformLocation(aws.StringValue(partition.StorageDescriptor.Location)))
		partition.StorageDescriptor.SerdeInfo.SetName(serde)
		if symlink {
			partition.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
		}
		switch difference {
		case catalog.DifferenceTypeRemoved:
			removePartitions = append(removePartitions, partition)
		case catalog.DifferenceTypeAdded:
			addPartitions = append(addPartitions, partition)
		default:
			alterPartitions = append(alterPartitions, partition)
		}
	})

	err = g.updateTable(toDB, table)
	if err != nil {
		return err
	}
	if len(addPartitions) > 0 {
		err = g.addPartitions(toDB, toTable, addPartitions)
		if err != nil {
			return err
		}
	}
	if len(alterPartitions) > 0 {
		err = g.alterPartitions(toDB, toTable, alterPartitions)
		if err != nil {
			return err
		}
	}
	if len(removePartitions) > 0 {
		err = g.removePartitions(toDB, toTable, removePartitions)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *MSClient) copyOrMerge(fromDB, fromTable, toDB, toTable, serde string, symlink bool, transformLocation func(location string) string) error {
	table, _ := g.getTable(toDB, toTable)

	if table == nil {
		return g.copy(fromDB, fromTable, toDB, toTable, serde, symlink, transformLocation)
	}
	return g.merge(fromDB, fromTable, toDB, toTable, serde, symlink, transformLocation)
}

func (g *MSClient) CopyOrMerge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde string, partition []string) error {

	if partition != nil && len(partition) > 0 {
		return g.CopyPartition(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde, partition)
	}
	transformLocation := func(location string) string {
		return metastore.TransformLocation(location, fromBranch, toBranch)
	}
	return g.copyOrMerge(fromDB, fromTable, toDB, toTable, serde, false, transformLocation)
}

func (g *MSClient) CopyOrMergeToSymlink(fromDB, fromTable, toDB, toTable, locationPrefix string) error {

	transformLocation := func(location string) string {
		return metastore.GetSymlinkLocation(location, locationPrefix)
	}
	return g.copyOrMerge(fromDB, fromTable, toDB, toTable, "", true, transformLocation)

}

func (g *MSClient) Diff(fromDB, fromTable, toDB, toTable string) (*metastore.MetaDiff, error) {

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

func (g *MSClient) getPartitionsDiff(FromDB string, fromTable string, toDB string, toTable string) (catalog.Differences, error) {
	partitions, err := g.getAllPartitions(FromDB, fromTable)
	if err != nil {
		return nil, err
	}
	toPartitions, err := g.getAllPartitions(toDB, toTable)
	if err != nil {
		return nil, err
	}

	partitionIter := NewPartitionIter(partitions)
	toPartitionIter := NewPartitionIter(toPartitions)
	return metastore.Diff(partitionIter, toPartitionIter), nil
}

func (g *MSClient) getColumnDiff(fromDB, fromTable, toDB, toTable string) (catalog.Differences, error) {
	tableFrom, err := g.getTable(fromDB, fromTable)
	if err != nil {
		return nil, err
	}
	tableTo, err := g.getTable(toDB, toTable)
	if err != nil {
		return nil, err
	}

	colsIter := NewColumnIter(tableFrom.StorageDescriptor.Columns)
	colsToIter := NewColumnIter(tableTo.StorageDescriptor.Columns)
	return metastore.Diff(colsIter, colsToIter), nil
}

func (g *MSClient) CopyPartition(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde string, partition []string) error {
	p1, err := g.getPartition(fromDB, fromTable, partition)
	if err != nil {
		return err
	}
	p2, _ := g.getPartition(toDB, toTable, partition)
	p1.SetDatabaseName(toDB)
	p1.SetTableName(toTable)
	p1.StorageDescriptor.SerdeInfo.SetName(serde)
	p1.StorageDescriptor.SetLocation(metastore.TransformLocation(aws.StringValue(p1.StorageDescriptor.Location), fromBranch, toBranch))
	if p2 == nil {
		err = g.addPartition(toDB, toTable, p1)
	} else {
		err = g.alterPartition(toDB, toTable, p1)
	}
	return err
}
