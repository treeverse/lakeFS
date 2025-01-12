package glue

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/treeverse/lakefs/pkg/metastore"
)

func serDeGlueToLocal(g *types.SerDeInfo) *metastore.SerDeInfo {
	if g == nil {
		return nil
	}
	return &metastore.SerDeInfo{
		Name:             aws.ToString(g.Name),
		SerializationLib: aws.ToString(g.SerializationLibrary),
		Parameters:       g.Parameters,
	}
}

func skewedGlueToLocal(g *types.SkewedInfo) *metastore.SkewedInfo {
	if g == nil {
		return &metastore.SkewedInfo{}
	}
	return &metastore.SkewedInfo{
		SkewedColNames:             g.SkewedColumnNames,
		SkewedColValues:            [][]string{g.SkewedColumnValues}, // TODO(Guys): validate this hive uses [][]string glue uses []string (????)
		SkewedColValueLocationMaps: g.SkewedColumnValueLocationMaps,
	}
}

func sortColumnsGlueToLocal(columns []types.Order) []*metastore.Order {
	res := make([]*metastore.Order, len(columns))
	for i, column := range columns {
		res[i] = &metastore.Order{
			Col:   aws.ToString(column.Column),
			Order: int(column.SortOrder),
		}
	}
	return res
}

func columnsGlueToLocal(columns []types.Column) []*metastore.FieldSchema {
	res := make([]*metastore.FieldSchema, len(columns))
	for i, column := range columns {
		res[i] = &metastore.FieldSchema{
			Name:    aws.ToString(column.Name),
			Type:    aws.ToString(column.Type),
			Comment: aws.ToString(column.Comment),
		}
	}
	return res
}

func TableGlueToLocal(glueTable *types.Table) *metastore.Table {
	sd := SDGlueToLocal(glueTable.StorageDescriptor)
	ht := &metastore.Table{
		TableName:        aws.ToString(glueTable.Name),
		DBName:           aws.ToString(glueTable.DatabaseName),
		Owner:            aws.ToString(glueTable.Owner),
		CreateTime:       aws.ToTime(glueTable.CreateTime).Unix(),     // TODO(Guys): check if this OK
		LastAccessTime:   aws.ToTime(glueTable.LastAccessTime).Unix(), // TODO(Guys): check if this OK
		Retention:        int(glueTable.Retention),                    // TODO(Guys): check if this OK
		Sd:               sd,
		PartitionKeys:    columnsGlueToLocal(glueTable.PartitionKeys),
		Parameters:       glueTable.Parameters,
		ViewOriginalText: aws.ToString(glueTable.ViewOriginalText),
		ViewExpandedText: aws.ToString(glueTable.ViewExpandedText),
		TableType:        aws.ToString(glueTable.TableType),
		RewriteEnabled:   nil,
		Temporary:        false,
	}
	return ht
}

func TablesGlueToLocal(glueTables []types.Table) []*metastore.Table {
	tables := make([]*metastore.Table, len(glueTables))
	for i := range glueTables {
		tables[i] = TableGlueToLocal(&glueTables[i])
	}
	return tables
}

func PartitionGlueToLocal(gluePartition *types.Partition) *metastore.Partition {
	sd := SDGlueToLocal(gluePartition.StorageDescriptor)
	partition := &metastore.Partition{
		Values:              gluePartition.Values,
		DBName:              aws.ToString(gluePartition.DatabaseName),
		TableName:           aws.ToString(gluePartition.TableName),
		CreateTime:          int(aws.ToTime(gluePartition.CreationTime).Unix()),
		LastAccessTime:      int(aws.ToTime(gluePartition.LastAccessTime).Unix()),
		Sd:                  sd,
		Parameters:          gluePartition.Parameters,
		AWSLastAnalyzedTime: gluePartition.LastAnalyzedTime,
	}
	return partition
}

func PartitionsGlueToLocal(gluePartitions []types.Partition) []*metastore.Partition {
	partitions := make([]*metastore.Partition, len(gluePartitions))
	for i := range gluePartitions {
		partitions[i] = PartitionGlueToLocal(&gluePartitions[i])
	}
	return partitions
}

func serDeLocalToGlue(info *metastore.SerDeInfo) *types.SerDeInfo {
	if info == nil {
		return nil
	}

	// glue cannot have an empty name
	name := "default"
	if info.Name != "" {
		name = info.Name
	}

	return &types.SerDeInfo{
		Name:                 aws.String(name),
		Parameters:           info.Parameters,
		SerializationLibrary: aws.String(info.SerializationLib),
	}
}

func skewedLocalToGlue(info *metastore.SkewedInfo) *types.SkewedInfo {
	if info == nil {
		return &types.SkewedInfo{}
	}
	return &types.SkewedInfo{
		SkewedColumnNames:             info.SkewedColNames,
		SkewedColumnValueLocationMaps: info.SkewedColValueLocationMaps,
		SkewedColumnValues:            info.AWSSkewedColValues, // TODO(Guys): validate this hive uses [][]string glue uses []string (????)
	}
}

func sortColumnsLocalToGlue(columns []*metastore.Order) []types.Order {
	res := make([]types.Order, 0, len(columns))
	for _, column := range columns {
		res = append(res, types.Order{
			Column:    aws.String(column.Col),
			SortOrder: int32(column.Order), //nolint:gosec
		})
	}
	return res
}

func columnsLocalToGlue(columns []*metastore.FieldSchema) []types.Column {
	res := make([]types.Column, 0, len(columns))
	for _, column := range columns {
		res = append(res, types.Column{
			Comment: aws.String(column.Comment),
			Name:    aws.String(column.Name),
			// Parameters: nil,
			Type: aws.String(column.Type),
		})
	}
	return res
}

func DatabaseLocalToGlue(db *metastore.Database) *types.DatabaseInput {
	return &types.DatabaseInput{
		// CreateTableDefaultPermissions: db.,
		Description:    aws.String(db.Description),
		LocationUri:    aws.String(db.LocationURI),
		Name:           aws.String(db.Name),
		Parameters:     db.Parameters,
		TargetDatabase: db.AWSTargetDatabase,
	}
}

func DatabasesGlueToLocal(glueDatabases []types.Database) []*metastore.Database {
	databases := make([]*metastore.Database, len(glueDatabases))
	for i := range glueDatabases {
		databases[i] = DatabaseGlueToLocal(&glueDatabases[i])
	}
	return databases
}

func DatabaseGlueToLocal(db *types.Database) *metastore.Database {
	return &metastore.Database{
		Name:        aws.ToString(db.Name),
		Description: aws.ToString(db.Description),
		LocationURI: aws.ToString(db.LocationUri),
		Parameters:  db.Parameters,
	}
}

func TableLocalToGlue(table *metastore.Table) *types.TableInput {
	sd := SDLocalToGlue(table.Sd)
	targetTable, _ := table.AWSTargetTable.(*types.TableIdentifier)
	ht := &types.TableInput{
		Description:      table.AWSDescription,
		LastAccessTime:   localToAWSTime(table.LastAccessTime), // TODO(Guys): check if this OK
		LastAnalyzedTime: table.AWSLastAnalyzedTime,
		Name:             aws.String(table.TableName),
		Owner:            aws.String(table.Owner),
		Parameters:       table.Parameters,
		PartitionKeys:    columnsLocalToGlue(table.PartitionKeys),
		//  TODO(Guys): check if this OK
		Retention:         int32(table.Retention), ////nolint:gosec
		StorageDescriptor: sd,
		TableType:         aws.String(table.TableType),
		TargetTable:       targetTable,
		ViewExpandedText:  aws.String(table.ViewExpandedText),
		ViewOriginalText:  aws.String(table.ViewOriginalText),
	}
	return ht
}

func PartitionLocalToGlue(partition *metastore.Partition) *types.PartitionInput {
	sd := SDLocalToGlue(partition.Sd)
	ht := &types.PartitionInput{
		// TODO(Guys): check if this OK
		LastAccessTime:    localToAWSTime(int64(partition.LastAccessTime)), //nolint:gosec
		LastAnalyzedTime:  partition.AWSLastAnalyzedTime,
		Parameters:        partition.Parameters,
		StorageDescriptor: sd,
		Values:            partition.Values,
	}
	return ht
}

func PartitionsLocalToGlue(partitions []*metastore.Partition) []*types.PartitionInput {
	gluePartitions := make([]*types.PartitionInput, len(partitions))
	for i, partition := range partitions {
		gluePartitions[i] = PartitionLocalToGlue(partition)
	}
	return gluePartitions
}

func SDLocalToGlue(sd *metastore.StorageDescriptor) *types.StorageDescriptor {
	if sd == nil {
		return nil
	}
	schemaRef, _ := sd.AWSSchemaReference.(*types.SchemaReference)
	return &types.StorageDescriptor{
		BucketColumns:          sd.BucketCols,
		Columns:                columnsLocalToGlue(sd.Cols),
		Compressed:             sd.Compressed,
		InputFormat:            aws.String(sd.InputFormat),
		Location:               aws.String(sd.Location),
		NumberOfBuckets:        int32(sd.NumBuckets), //nolint:gosec
		OutputFormat:           aws.String(sd.OutputFormat),
		Parameters:             sd.Parameters,
		SchemaReference:        schemaRef,
		SerdeInfo:              serDeLocalToGlue(sd.SerdeInfo),
		SkewedInfo:             skewedLocalToGlue(sd.SkewedInfo),
		SortColumns:            sortColumnsLocalToGlue(sd.SortCols),
		StoredAsSubDirectories: aws.ToBool(sd.StoredAsSubDirectories),
	}
}

func SDGlueToLocal(sd *types.StorageDescriptor) *metastore.StorageDescriptor {
	if sd == nil {
		return nil
	}
	return &metastore.StorageDescriptor{
		Cols:                   columnsGlueToLocal(sd.Columns),
		Location:               aws.ToString(sd.Location),
		InputFormat:            aws.ToString(sd.InputFormat),
		OutputFormat:           aws.ToString(sd.OutputFormat),
		Compressed:             sd.Compressed,
		NumBuckets:             int(sd.NumberOfBuckets),
		SerdeInfo:              serDeGlueToLocal(sd.SerdeInfo),
		BucketCols:             sd.BucketColumns,
		SortCols:               sortColumnsGlueToLocal(sd.SortColumns),
		Parameters:             sd.Parameters,
		SkewedInfo:             skewedGlueToLocal(sd.SkewedInfo),
		StoredAsSubDirectories: aws.Bool(sd.StoredAsSubDirectories),
		AWSSchemaReference:     sd.SchemaReference,
	}
}

func localToAWSTime(t int64) *time.Time {
	tm := time.UnixMilli(t).UTC()
	return &tm
}
