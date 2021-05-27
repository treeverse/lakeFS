package metastore

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/logging"
)

type ReadClient interface {
	GetTable(ctx context.Context, dbName string, tableName string) (r *Table, err error)
	HasTable(ctx context.Context, dbName string, tableName string) (hasTable bool, err error)
	GetPartitions(ctx context.Context, dbName string, tableName string) (r []*Partition, err error)
	GetPartition(ctx context.Context, dbName string, tableName string, values []string) (r *Partition, err error)
	GetDatabase(ctx context.Context, name string) (r *Database, err error)
	GetDatabases(ctx context.Context, pattern string) (databases []*Database, err error)
	GetTables(ctx context.Context, dbName string, pattern string) (tables []*Table, err error)
}

type WriteClient interface {
	CreateTable(ctx context.Context, tbl *Table) error
	AlterTable(ctx context.Context, dbName string, tableName string, newTable *Table) error
	AddPartitions(ctx context.Context, tableName string, dbName string, newParts []*Partition) error
	AlterPartitions(ctx context.Context, dbName string, tableName string, newPartitions []*Partition) error
	AlterPartition(ctx context.Context, dbName string, tableName string, partition *Partition) error
	AddPartition(ctx context.Context, tableName string, dbName string, newPartition *Partition) error
	DropPartition(ctx context.Context, dbName string, tableName string, values []string) error
	CreateDatabaseIfNotExists(ctx context.Context, database *Database) error
	NormalizeDBName(name string) string // NormalizeDBName changes the db name to be a valid name for the client
	GetDBLocation(dbName string) string // getDBLocation returns the expected locationURI of the database
}

type Client interface {
	ReadClient
	WriteClient
}

func CopyOrMerge(ctx context.Context, fromClient Client, toClient Client, fromDB, fromTable, toDB, toTable, toBranch, serde string, partition []string) error {
	transformLocation := func(location string) (string, error) {
		return ReplaceBranchName(location, toBranch)
	}
	return copyOrMergeWithTransformLocation(ctx, fromClient, toClient, fromDB, fromTable, toDB, toTable, serde, partition, transformLocation)
}

func copyOrMergeWithTransformLocation(ctx context.Context, fromClient, toClient Client, fromDB, fromTable, toDB, toTable, serde string, partition []string, transformLocation func(location string) (string, error)) error {
	log := logging.Default().WithFields(logging.Fields{
		"from_db":       fromDB,
		"from_table":    fromTable,
		"to_db":         toDB,
		"to_table":      toTable,
		"serde":         serde,
		"partition_len": len(partition),
	})
	if len(partition) > 0 {
		log.Debug("CopyPartition")
		return CopyPartition(ctx, fromClient, toClient, fromDB, fromTable, toDB, toTable, serde, partition, transformLocation)
	}
	hasTable, err := toClient.HasTable(ctx, toDB, toTable)
	if err != nil {
		return err
	}
	if !hasTable {
		log.Debug("Copy")
		table, err := fromClient.GetTable(ctx, fromDB, fromTable)
		if err != nil {
			return err
		}
		partitions, err := fromClient.GetPartitions(ctx, fromDB, fromTable)
		if err != nil {
			return err
		}
		return Copy(ctx, table, partitions, toDB, toTable, serde, toClient, transformLocation)
	}
	log.Debug("Merge")
	table, err := fromClient.GetTable(ctx, fromDB, fromTable)
	if err != nil {
		return err
	}
	partitions, err := fromClient.GetPartitions(ctx, fromDB, fromTable)
	if err != nil {
		return err
	}
	partitionCollection := NewPartitionCollection(partitions)
	return Merge(ctx, table, partitionCollection, toDB, toTable, serde, toClient, transformLocation)
}

func CopyOrMergeFromValues(ctx context.Context, fromClient Client, fTable *Table, toClient Client, fromDB, fromTable, toDB, toTable, serde string, transformLocation func(location string) (string, error)) error {
	hasTable, err := toClient.HasTable(ctx, toDB, toTable)
	if err != nil {
		return err
	}
	partitions, err := fromClient.GetPartitions(ctx, fromDB, fromTable)
	if err != nil {
		return err
	}
	if !hasTable {
		return Copy(ctx, fTable, partitions, toDB, toTable, serde, toClient, transformLocation)
	}
	partitionCollection := NewPartitionCollection(partitions)
	return Merge(ctx, fTable, partitionCollection, toDB, toTable, serde, toClient, transformLocation)
}

func CopyOrMergeAll(ctx context.Context, fromClient, toClient Client, schemaFilter, tableFilter, toBranch string, continueOnError bool) error {
	databases, err := fromClient.GetDatabases(ctx, schemaFilter)
	if err != nil {
		return err
	}
	for _, database := range databases {
		fromDBName := database.Name
		toDBName := toClient.NormalizeDBName(database.Name)
		database.Name = toDBName
		database.LocationURI = toClient.GetDBLocation(toDBName)
		err = toClient.CreateDatabaseIfNotExists(ctx, database)
		if err != nil {
			return err
		}
		tables, err := fromClient.GetTables(ctx, fromDBName, tableFilter)
		if err != nil {
			return err
		}
		transformLocation := func(location string) (string, error) {
			location, err := ReplaceBranchName(location, toBranch)
			if err != nil {
				return "", err
			}
			return location, err
		}
		for _, table := range tables {
			tableName := table.TableName
			fmt.Printf("copy table %s.%s -> %s.%s\n", fromDBName, tableName, toDBName, tableName)
			err = CopyOrMergeFromValues(ctx, fromClient, table, toClient, fromDBName, tableName, toDBName, tableName, tableName, transformLocation)
			if err != nil {
				if !continueOnError {
					return err
				}
				fmt.Println(err)
			}
		}
	}
	return nil
}

func Copy(ctx context.Context, fromTable *Table, partitions []*Partition, toDB, toTable, serde string, toClient WriteClient, transformLocation func(location string) (string, error)) error {
	isSparkSQLTable := fromTable.isSparkSQLTable()
	err := fromTable.Update(toDB, toTable, serde, transformLocation, isSparkSQLTable)
	if err != nil {
		return err
	}
	for _, partition := range partitions {
		err := partition.Update(toDB, toTable, serde, transformLocation, isSparkSQLTable)
		if err != nil {
			return err
		}
	}
	err = toClient.CreateTable(ctx, fromTable)
	if err != nil {
		return err
	}
	err = toClient.AddPartitions(ctx, toTable, toDB, partitions)
	return err
}

func Merge(ctx context.Context, table *Table, partitionIter Collection, toDB, toTable, serde string, toClient Client, transformLocation func(location string) (string, error)) error {
	isSparkSQLTable := table.isSparkSQLTable()
	err := table.Update(toDB, toTable, serde, transformLocation, isSparkSQLTable)
	if err != nil {
		return err
	}
	toPartitions, err := toClient.GetPartitions(ctx, toDB, toTable)
	if err != nil {
		return err
	}
	toPartitionIter := NewPartitionCollection(toPartitions)
	var addPartitions, removePartitions, alterPartitions []*Partition
	err = DiffIterable(partitionIter, toPartitionIter, func(difference catalog.DifferenceType, value interface{}, _ string) error {
		partition, ok := value.(*Partition)
		if !ok {
			return fmt.Errorf("%w in diffIterable call. expected to get * Partition, but got: %T", ErrExpectedType, value)
		}
		err = partition.Update(toDB, toTable, serde, transformLocation, isSparkSQLTable)
		if err != nil {
			return err
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

	err = toClient.AlterTable(ctx, toDB, toTable, table)
	if err != nil {
		return err
	}
	err = toClient.AddPartitions(ctx, toTable, toDB, addPartitions)
	if err != nil {
		return err
	}
	err = toClient.AlterPartitions(ctx, toDB, toTable, alterPartitions)
	if err != nil {
		return err
	}
	// drop one by one
	for _, partition := range removePartitions {
		values := partition.Values
		err = toClient.DropPartition(ctx, toDB, toTable, values)
		if err != nil {
			return err
		}
	}
	return nil
}

func CopyPartition(ctx context.Context, fromClient ReadClient, toClient Client, fromDB, fromTable, toDB, toTable, serde string, partition []string, transformLocation func(location string) (string, error)) error {
	t1, err := fromClient.GetTable(ctx, fromDB, fromTable)
	if err != nil {
		return err
	}
	p1, err := fromClient.GetPartition(ctx, fromDB, fromTable, partition)
	if err != nil {
		return err
	}
	p2, err := toClient.GetPartition(ctx, toDB, toTable, partition)
	if err != nil {
		return err
	}
	err = p1.Update(toDB, toTable, serde, transformLocation, t1.isSparkSQLTable())
	if err != nil {
		return err
	}
	if p2 == nil {
		err = toClient.AddPartition(ctx, "", "", p1)
	} else {
		err = toClient.AlterPartition(ctx, toDB, toTable, p1)
	}
	return err
}

func GetDiff(ctx context.Context, fromClient, toClient ReadClient, fromDB, fromTable, toDB, toTable string) (*MetaDiff, error) {
	diffColumns, err := getColumnDiff(ctx, fromClient, toClient, fromDB, fromTable, toDB, toTable)
	if err != nil {
		return nil, err
	}
	partitionDiff, err := getPartitionsDiff(ctx, fromClient, toClient, fromDB, fromTable, toDB, toTable)
	if err != nil {
		return nil, err
	}
	return &MetaDiff{
		PartitionDiff: partitionDiff,
		ColumnsDiff:   diffColumns,
	}, nil
}

func getPartitionsDiff(ctx context.Context, fromClient, toClient ReadClient, fromDB string, fromTable string, toDB string, toTable string) (catalog.Differences, error) {
	partitions, err := fromClient.GetPartitions(ctx, fromDB, fromTable)
	if err != nil {
		return nil, err
	}
	partitionIter := NewPartitionCollection(partitions)
	toPartitions, err := toClient.GetPartitions(ctx, toDB, toTable)
	if err != nil {
		return nil, err
	}
	toPartitionIter := NewPartitionCollection(toPartitions)
	return Diff(partitionIter, toPartitionIter)
}

func getColumnDiff(ctx context.Context, fromClient, toClient ReadClient, fromDB, fromTable, toDB, toTable string) (catalog.Differences, error) {
	table, err := fromClient.GetTable(ctx, fromDB, fromTable)
	if err != nil {
		return nil, err
	}
	colsIter := NewColumnCollection(table.Sd.Cols)

	toTbl, err := toClient.GetTable(ctx, toDB, toTable)
	if err != nil {
		return nil, err
	}
	toColumns := toTbl.Sd.Cols // TODO(Guys): change name
	toColsIter := NewColumnCollection(toColumns)

	return Diff(colsIter, toColsIter)
}

func CopyOrMergeToSymlink(ctx context.Context, client Client, fromDB, fromTable, toDB, toTable, locationPrefix string) error {
	transformLocation := func(location string) (string, error) {
		return GetSymlinkLocation(location, locationPrefix)
	}
	return copyOrMergeWithTransformLocation(ctx, client, client, fromDB, fromTable, toDB, toTable, "", nil, transformLocation)
}
