package glue

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/metastore"
)

type GlueMSClient struct {
	client    glueiface.GlueAPI
	catalogID *string /**/
}

const MaxParts = 1000 // max possible 1000

func GetGlueService(cfg *aws.Config) *glue.Glue {
	sess := session.Must(session.NewSession(cfg))
	sess.ClientConfig("glue")
	return glue.New(sess)
}

func NewGlueMSClient(svc glueiface.GlueAPI, catalogID string) *GlueMSClient {
	return &GlueMSClient{
		client:    svc,
		catalogID: aws.String(catalogID),
	}
}

func (g *GlueMSClient) getTable(dbName string, tblName string) (*glue.TableData, error) {
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

func (g *GlueMSClient) createTable(dbName string, tbl *glue.TableData) error {
	_, err := g.client.CreateTable(&glue.CreateTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		TableInput:   getAsTableInput(tbl),
	})
	return err
}

func (g *GlueMSClient) updateTable(dbName string, tbl *glue.TableData) error {
	_, err := g.client.UpdateTable(&glue.UpdateTableInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		TableInput:   getAsTableInput(tbl),
	})
	return err
}
func (g *GlueMSClient) getPartition(dbName, tableName string, partitionValues []string) (*glue.Partition, error) {
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

func (g *GlueMSClient) getPartitions(dbName, tableName string, nextToken *string, maxParts int16) (*glue.GetPartitionsOutput, error) {
	return g.client.GetPartitions(&glue.GetPartitionsInput{
		CatalogId:    g.catalogID,
		DatabaseName: aws.String(dbName),
		MaxResults:   aws.Int64(int64(maxParts)),
		NextToken:    nextToken,
		TableName:    aws.String(tableName),
	})
}

func (g *GlueMSClient) getAllPartitions(dbName, tableName string) ([]*glue.Partition, error) {
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

func (g *GlueMSClient) addPartition(dbName, tableName string, partition *glue.Partition) error {
	_, err := g.client.CreatePartition(&glue.CreatePartitionInput{
		CatalogId:      g.catalogID,
		DatabaseName:   aws.String(dbName),
		PartitionInput: getAsPartitionInput(partition),
		TableName:      aws.String(tableName),
	})
	return err
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
	_, err := g.client.BatchCreatePartition(&glue.BatchCreatePartitionInput{
		CatalogId:          g.catalogID,
		DatabaseName:       aws.String(dbName),
		PartitionInputList: partitionList,
		TableName:          aws.String(tableName),
	})
	return err
}

func (g *GlueMSClient) alterPartition(dbName, tableName string, partition *glue.Partition) error {
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

func (g *GlueMSClient) alterPartitions(dbName, tableName string, partitions []*glue.Partition) error {
	// No batch alter partitions we will need to do it one by one
	for _, partition := range partitions {
		err := g.alterPartition(dbName, tableName, partition)
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

func (g *GlueMSClient) copyPartitions(fromDBName, fromTable, toDBName, toTable string, transformLocation func(location string) string, symlink bool) error {
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

func (g *GlueMSClient) copyTable(fromDBName, fromTable, toDBName, toTable string, transformLocation func(loaction string) string, symlink bool) error {
	t, err := g.client.GetTable(&glue.GetTableInput{
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
	table.StorageDescriptor.SetLocation(transformLocation(aws.StringValue(table.StorageDescriptor.Location)))
	if symlink {
		table.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
		table.StorageDescriptor.SetLocation(transformLocation(""))
	}
	t.SetTable(table)
	err = g.createTable(toDBName, table)
	if err != nil {
		return err
	}
	return nil
}

func (g *GlueMSClient) copy(fromDB, fromTable, toDB, toTable string, transformLocation func(location string) string, symlink bool) error {

	err := g.copyTable(fromDB, fromTable, toDB, toTable, transformLocation, symlink)
	if err != nil {
		return err
	}
	err = g.copyPartitions(fromDB, fromTable, toDB, toTable, transformLocation, symlink)
	return err
}

func (g *GlueMSClient) merge(FromDB, fromTable, toDB, toTable string, transformLocation func(location string) string, symlink bool) error {

	table, err := g.getTable(FromDB, fromTable)
	if err != nil {
		return err
	}
	table.SetDatabaseName(toDB)
	table.SetName(toTable)
	table.StorageDescriptor.SerdeInfo.SetName(toTable) //todo double check this, maybe serde should be somehting else
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
	metastore.Diff(partitionIter, toPartitionIter, func(difference catalog.DifferenceType, iter metastore.ComparableIterator) {
		partition := iter.(*PartitionIter).getCurrent()
		partition.SetDatabaseName(toDB)
		partition.SetTableName(toTable)
		partition.StorageDescriptor.SetLocation(transformLocation(aws.StringValue(partition.StorageDescriptor.Location)))
		partition.StorageDescriptor.SerdeInfo.SetName(toTable)
		if symlink {
			partition.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
		}
		switch difference {
		case catalog.DifferenceTypeRemoved:
			removePartitions = append(removePartitions, iter.(*PartitionIter).getCurrent())
		case catalog.DifferenceTypeAdded:
			addPartitions = append(addPartitions, iter.(*PartitionIter).getCurrent())
		default:
			alterPartitions = append(alterPartitions, iter.(*PartitionIter).getCurrent())
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

func (g *GlueMSClient) copyOrMerge(fromDB, fromTable, toDB, toTable string, transformLocation func(location string) string, symlink bool) error {
	table, _ := g.getTable(toDB, toTable)

	if table == nil {
		return g.copy(fromDB, fromTable, toDB, toTable, transformLocation, symlink)
	}
	return g.merge(fromDB, fromTable, toDB, toTable, transformLocation, symlink)
}

func (g *GlueMSClient) CopyOrMerge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch string) error {
	transformLocation := func(location string) string {
		return metastore.TransformLocation(location, fromBranch, toBranch)
	}
	return g.copyOrMerge(fromDB, fromTable, toDB, toTable, transformLocation, false)
}

func (g *GlueMSClient) CopyOrMergeToSymlink(fromDB, fromTable, toDB, toTable, locationPrefix string) error {

	transformLocation := func(location string) string {
		return metastore.GetSymlinkLocation(location, locationPrefix)
	}
	return g.copyOrMerge(fromDB, fromTable, toDB, toTable, transformLocation, true)

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

func (g *GlueMSClient) getPartitionsDiff(FromDB string, fromTable string, toDB string, toTable string) (catalog.Differences, error) {
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
	return metastore.GetDiff(partitionIter, toPartitionIter), nil
}

func (g *GlueMSClient) getColumnDiff(fromDB, fromTable, toDB, toTable string) (catalog.Differences, error) {
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
	return metastore.GetDiff(colsIter, colsToIter), nil
}

func (g *GlueMSClient) CopyPartition(fromDB, fromTable, fromBranch, toDB, toTable, toBranch string, partition []string) error {
	p1, err := g.getPartition(fromDB, fromTable, partition)
	if err != nil {
		return err
	}
	p2, _ := g.getPartition(toDB, toTable, partition)

	p1.SetDatabaseName(toDB)
	p1.SetTableName(toTable)
	p1.StorageDescriptor.SerdeInfo.SetName(toTable)
	p1.StorageDescriptor.SetLocation(metastore.TransformLocation(aws.StringValue(p1.StorageDescriptor.Location), fromBranch, toBranch))
	if p2 == nil {
		err = g.addPartition(toDB, toTable, p1)
	} else {
		err = g.alterPartition(toDB, toTable, p1)
	}
	return err
}
