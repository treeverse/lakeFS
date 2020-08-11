package glue

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/metastore"
)

type MSClient struct {
	client    glueiface.GlueAPI
	catalogID string
}

const MaxParts = 1000 // max possible 1000

func NewMSClient(cfg *aws.Config, catalogID string) (*MSClient, error) {
	sess := session.Must(session.NewSession(cfg))
	sess.ClientConfig("glue")
	gl := glue.New(sess)
	return &MSClient{
		client:    gl,
		catalogID: catalogID,
	}, nil
}

func (g *MSClient) getTable(dbName string, tblName string) (*glue.TableData, error) {
	table, err := g.client.GetTable(&glue.GetTableInput{
		CatalogId:    aws.String(g.catalogID),
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
		CatalogId:    aws.String(g.catalogID),
		DatabaseName: aws.String(dbName),
		TableInput:   getAsTableInput(tbl),
	})
	return err
}

func (g *MSClient) updateTable(dbName string, tbl *glue.TableData) error {
	_, err := g.client.UpdateTable(&glue.UpdateTableInput{
		CatalogId:    aws.String(g.catalogID),
		DatabaseName: aws.String(dbName),
		TableInput:   getAsTableInput(tbl),
	})
	return err
}
func (g *MSClient) getPartition(dbName, tableName string, partitionValues []string) (*glue.Partition, error) {
	output, err := g.client.GetPartition(&glue.GetPartitionInput{
		CatalogId:       aws.String(g.catalogID),
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
		CatalogId:    aws.String(g.catalogID),
		DatabaseName: aws.String(dbName),
		MaxResults:   aws.Int64(int64(maxParts)),
		NextToken:    nextToken,
		TableName:    aws.String(tableName),
	})
}

func (g *MSClient) getAllPartitions(dbName, tableName string) ([]*glue.Partition, error) {
	var nextToken *string
	var allPartitions []*glue.Partition
	for {
		getPartitionsOutput, err := g.getPartitions(dbName, tableName, nextToken, MaxParts)
		if err != nil {
			return nil, err
		}
		nextToken = getPartitionsOutput.NextToken
		partitions := getPartitionsOutput.Partitions
		allPartitions = append(allPartitions, partitions...)
		if nextToken == nil {
			break
		}
	}
	return allPartitions, nil
}

func (g *MSClient) addPartition(dbName, tableName string, partition *glue.Partition) error {
	_, err := g.client.CreatePartition(&glue.CreatePartitionInput{
		CatalogId:      aws.String(g.catalogID),
		DatabaseName:   aws.String(dbName),
		PartitionInput: getAsPartitionInput(partition),
		TableName:      aws.String(tableName),
	})
	return err
}

func (g *MSClient) addPartitions(dbName, tableName string, partitions []*glue.Partition) error {
	partitionList := make([]*glue.PartitionInput, 0, len(partitions))
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
		CatalogId:          aws.String(g.catalogID),
		DatabaseName:       aws.String(dbName),
		PartitionInputList: partitionList,
		TableName:          aws.String(tableName),
	})
	return err
}

func (g *MSClient) alterPartition(dbName, tableName string, partition *glue.Partition) error {
	// No batch alter partitions we will need to do it one by one

	_, err := g.client.UpdatePartition(&glue.UpdatePartitionInput{
		CatalogId:    aws.String(g.catalogID),
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
	return err
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
	partitionsToDelete := make([]*glue.PartitionValueList, 0, len(partitions))
	for _, partition := range partitions {
		partitionsToDelete = append(partitionsToDelete, &glue.PartitionValueList{Values: partition.Values})
	}
	_, err := g.client.BatchDeletePartition(&glue.BatchDeletePartitionInput{
		CatalogId:          aws.String(g.catalogID),
		DatabaseName:       aws.String(dbName),
		PartitionsToDelete: partitionsToDelete,
		TableName:          aws.String(tableName),
	})
	return err
}

func (g *MSClient) copyPartitions(fromDBName, fromTable, toDBName, toTable, serde string, symlink bool, transformLocation func(location string) (string, error)) error {
	var nextToken *string
	for {
		getPartitionsOutput, err := g.getPartitions(fromDBName, fromTable, nextToken, MaxParts)
		if err != nil {
			return err
		}
		nextToken = getPartitionsOutput.NextToken
		partitions := getPartitionsOutput.Partitions
		for _, partition := range partitions {
			partition.SetTableName(toTable)
			partition.SetDatabaseName(toDBName)
			if partition.StorageDescriptor != nil {
				if partition.StorageDescriptor.SerdeInfo != nil {
					partition.StorageDescriptor.SerdeInfo.SetName(serde)
				}
				if symlink {
					partition.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
				}
				location, err := transformLocation(aws.StringValue(partition.StorageDescriptor.Location))
				if err != nil {
					return err
				}
				partition.StorageDescriptor.SetLocation(location)
			}
		}
		_ = getPartitionsOutput.SetPartitions(partitions)
		err = g.addPartitions(toDBName, toTable, partitions)
		if err != nil {
			return err
		}
		if nextToken == nil {
			break
		}
	}
	return nil
}

func (g *MSClient) copyTable(fromDB, fromTable, toDB, toTable, serde string, symlink bool, transformLocation func(location string) (string, error)) error {
	table, err := g.getTable(fromDB, fromTable)
	if err != nil {
		return err
	}
	table.SetDatabaseName(toDB)
	table.SetName(toTable)
	if table.StorageDescriptor != nil {
		if table.StorageDescriptor.SerdeInfo != nil {
			table.StorageDescriptor.SerdeInfo.SetName(serde)
		}
		if symlink {
			table.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
		}
		location, err := transformLocation(aws.StringValue(table.StorageDescriptor.Location))
		if err != nil {
			return err
		}
		table.StorageDescriptor.SetLocation(location)
	}
	err = g.createTable(toDB, table)
	return err
}

func (g *MSClient) copy(fromDB, fromTable, toDB, toTable, serde string, symlink bool, transformLocation func(location string) (string, error)) error {
	err := g.copyTable(fromDB, fromTable, toDB, toTable, serde, symlink, transformLocation)
	if err != nil {
		return err
	}
	err = g.copyPartitions(fromDB, fromTable, toDB, toTable, serde, symlink, transformLocation)
	return err
}

func (g *MSClient) merge(fromDB, fromTable, toDB, toTable, serde string, symlink bool, transformLocation func(location string) (string, error)) error {
	table, err := g.getTable(fromDB, fromTable)
	if err != nil {
		return err
	}
	table.SetDatabaseName(toDB)
	table.SetName(toTable)
	if table.StorageDescriptor != nil {
		if table.StorageDescriptor.SerdeInfo != nil {
			table.StorageDescriptor.SerdeInfo.SetName(serde)
		}
		table.StorageDescriptor.SerdeInfo.SetName(serde)

		location, err := transformLocation(aws.StringValue(table.StorageDescriptor.Location))
		if err != nil {
			return err
		}
		table.StorageDescriptor.SetLocation(location)
		if symlink {
			table.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
		}
	}
	partitions, err := g.getAllPartitions(fromDB, fromTable)
	if err != nil {
		return err
	}
	toPartitions, err := g.getAllPartitions(toDB, toTable)
	if err != nil {
		return err
	}

	partitionIter := NewPartitionCollection(partitions)
	toPartitionIter := NewPartitionCollection(toPartitions)
	var addPartitions, removePartitions, alterPartitions []*glue.Partition
	err = metastore.DiffIterable(partitionIter, toPartitionIter, func(difference catalog.DifferenceType, value interface{}, _ string) error {
		partition, ok := value.(*glue.Partition)
		if !ok {
			return fmt.Errorf("%w in diffIterable call. expected to get *glue.Partition, but got: %T", ErrUnexpectedValue, value)
		}
		partition.SetDatabaseName(toDB)
		partition.SetTableName(toTable)
		if partition.StorageDescriptor != nil {
			if partition.StorageDescriptor.SerdeInfo != nil {
				partition.StorageDescriptor.SerdeInfo.SetName(serde)
			}
			location, err := transformLocation(aws.StringValue(partition.StorageDescriptor.Location))
			if err != nil {
				return err
			}
			partition.StorageDescriptor.SetLocation(location)
			if symlink {
				partition.StorageDescriptor.SetInputFormat(metastore.SymlinkInputFormat)
			}
		}
		switch difference {
		case catalog.DifferenceTypeRemoved:
			removePartitions = append(removePartitions, partition)
		case catalog.DifferenceTypeAdded:
			addPartitions = append(addPartitions, partition)
		default:
			alterPartitions = append(alterPartitions, partition)
		}
		return nil
	})
	if err != nil {
		return err
	}
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

func (g *MSClient) copyOrMerge(fromDB, fromTable, toDB, toTable, serde string, symlink bool, transformLocation func(location string) (string, error)) error {
	table, err := g.getTable(toDB, toTable)
	if err != nil {
		if _, ok := err.(*glue.EntityNotFoundException); !ok {
			return err
		}
	}
	if table == nil {
		return g.copy(fromDB, fromTable, toDB, toTable, serde, symlink, transformLocation)
	}
	return g.merge(fromDB, fromTable, toDB, toTable, serde, symlink, transformLocation)
}

func (g *MSClient) CopyOrMerge(fromDB, fromTable, toDB, toTable, toBranch, serde string, partition []string) error {
	if len(partition) > 0 {
		return g.CopyPartition(fromDB, fromTable, toDB, toTable, toBranch, serde, partition)
	}
	transformLocation := func(location string) (string, error) {
		return metastore.ReplaceBranchName(location, toBranch)
	}
	return g.copyOrMerge(fromDB, fromTable, toDB, toTable, serde, false, transformLocation)
}

func (g *MSClient) CopyOrMergeToSymlink(fromDB, fromTable, toDB, toTable, locationPrefix string) error {
	transformLocation := func(location string) (string, error) {
		return metastore.GetSymlinkLocation(location, locationPrefix)
	}
	return g.copyOrMerge(fromDB, fromTable, toDB, toTable, toTable, true, transformLocation)
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

func (g *MSClient) getPartitionsDiff(fromDB string, fromTable string, toDB string, toTable string) (catalog.Differences, error) {
	partitions, err := g.getAllPartitions(fromDB, fromTable)
	if err != nil {
		return nil, err
	}
	toPartitions, err := g.getAllPartitions(toDB, toTable)
	if err != nil {
		return nil, err
	}

	partitionCollection := NewPartitionCollection(partitions)
	toPartitionCollection := NewPartitionCollection(toPartitions)
	return metastore.Diff(partitionCollection, toPartitionCollection)
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

	colsFrom := NewColumnCollection(tableFrom.StorageDescriptor.Columns)
	colsTo := NewColumnCollection(tableTo.StorageDescriptor.Columns)
	return metastore.Diff(colsFrom, colsTo)
}

func (g *MSClient) CopyPartition(fromDB, fromTable, toDB, toTable, toBranch, serde string, partition []string) error {
	p1, err := g.getPartition(fromDB, fromTable, partition)
	if err != nil {
		return err
	}
	p2, err := g.getPartition(toDB, toTable, partition)
	if err != nil {
		if _, ok := err.(*glue.EntityNotFoundException); !ok {
			return err
		}
	}
	p1.SetDatabaseName(toDB)
	p1.SetTableName(toTable)
	if p1.StorageDescriptor != nil {
		p1.StorageDescriptor.SerdeInfo.SetName(serde)
		location, err := metastore.ReplaceBranchName(aws.StringValue(p1.StorageDescriptor.Location), toBranch)
		if err != nil {
			return err
		}
		p1.StorageDescriptor.SetLocation(location)
	}
	if p2 == nil {
		err = g.addPartition(toDB, toTable, p1)
	} else {
		err = g.alterPartition(toDB, toTable, p1)
	}
	return err
}
