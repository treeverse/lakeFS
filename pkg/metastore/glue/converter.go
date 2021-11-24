package glue

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/treeverse/lakefs/pkg/metastore"
)

func serDeGlueToLocal(g *glue.SerDeInfo) *metastore.SerDeInfo {
	if g == nil {
		return nil
	}
	return &metastore.SerDeInfo{
		Name:             aws.StringValue(g.Name),
		SerializationLib: aws.StringValue(g.SerializationLibrary),
		Parameters:       aws.StringValueMap(g.Parameters),
	}
}

func skewedGlueToLocal(g *glue.SkewedInfo) *metastore.SkewedInfo {
	if g == nil {
		return &metastore.SkewedInfo{}
	}
	return &metastore.SkewedInfo{
		SkewedColNames:             aws.StringValueSlice(g.SkewedColumnNames),
		SkewedColValues:            [][]string{aws.StringValueSlice(g.SkewedColumnValues)}, // TODO(Guys): validate this hive uses [][]string glue uses []string (????)
		SkewedColValueLocationMaps: aws.StringValueMap(g.SkewedColumnValueLocationMaps),
	}
}

func sortColumnsGlueToLocal(columns []*glue.Order) []*metastore.Order {
	res := make([]*metastore.Order, len(columns))
	for i, column := range columns {
		res[i] = &metastore.Order{
			Col:   aws.StringValue(column.Column),
			Order: int(aws.Int64Value(column.SortOrder)),
		}
	}
	return res
}

func columnsGlueToLocal(columns []*glue.Column) []*metastore.FieldSchema {
	res := make([]*metastore.FieldSchema, len(columns))
	for i, column := range columns {
		res[i] = &metastore.FieldSchema{
			Name:    aws.StringValue(column.Name),
			Type:    aws.StringValue(column.Type),
			Comment: aws.StringValue(column.Comment),
		}
	}
	return res
}

func TableGlueToLocal(glueTable *glue.TableData) *metastore.Table {
	sd := SDGlueToLocal(glueTable.StorageDescriptor)
	ht := &metastore.Table{
		TableName:        aws.StringValue(glueTable.Name),
		DBName:           aws.StringValue(glueTable.DatabaseName),
		Owner:            aws.StringValue(glueTable.Owner),
		CreateTime:       aws.TimeValue(glueTable.CreateTime).Unix(),     // TODO(Guys): check if this OK
		LastAccessTime:   aws.TimeValue(glueTable.LastAccessTime).Unix(), // TODO(Guys): check if this OK
		Retention:        int(aws.Int64Value(glueTable.Retention)),       // TODO(Guys): check if this OK
		Sd:               sd,
		PartitionKeys:    columnsGlueToLocal(glueTable.PartitionKeys),
		Parameters:       aws.StringValueMap(glueTable.Parameters),
		ViewOriginalText: aws.StringValue(glueTable.ViewOriginalText),
		ViewExpandedText: aws.StringValue(glueTable.ViewExpandedText),
		TableType:        aws.StringValue(glueTable.TableType),
		RewriteEnabled:   nil,
		Temporary:        false,
	}
	return ht
}

func TablesGlueToLocal(glueTables []*glue.TableData) []*metastore.Table {
	tables := make([]*metastore.Table, len(glueTables))
	for i, table := range glueTables {
		tables[i] = TableGlueToLocal(table)
	}
	return tables
}

func PartitionGlueToLocal(gluePartition *glue.Partition) *metastore.Partition {
	sd := SDGlueToLocal(gluePartition.StorageDescriptor)
	partition := &metastore.Partition{
		Values:              aws.StringValueSlice(gluePartition.Values),
		DBName:              aws.StringValue(gluePartition.DatabaseName),
		TableName:           aws.StringValue(gluePartition.TableName),
		CreateTime:          int(aws.TimeValue(gluePartition.CreationTime).Unix()),
		LastAccessTime:      int(aws.TimeValue(gluePartition.LastAccessTime).Unix()),
		Sd:                  sd,
		Parameters:          aws.StringValueMap(gluePartition.Parameters),
		AWSLastAnalyzedTime: gluePartition.LastAnalyzedTime,
	}
	return partition
}

func PartitionsGlueToLocal(gluePartitions []*glue.Partition) []*metastore.Partition {
	partitions := make([]*metastore.Partition, len(gluePartitions))
	for i, partition := range gluePartitions {
		partitions[i] = PartitionGlueToLocal(partition)
	}
	return partitions
}

func serDeLocalToGlue(info *metastore.SerDeInfo) *glue.SerDeInfo {
	if info == nil {
		return nil
	}

	// glue cannot have empty name
	name := "default"
	if info.Name != "" {
		name = info.Name
	}

	return &glue.SerDeInfo{
		Name:                 aws.String(name),
		Parameters:           aws.StringMap(info.Parameters),
		SerializationLibrary: aws.String(info.SerializationLib),
	}
}

func skewedLocalToGlue(info *metastore.SkewedInfo) *glue.SkewedInfo {
	if info == nil {
		return &glue.SkewedInfo{}
	}
	return &glue.SkewedInfo{
		SkewedColumnNames:             aws.StringSlice(info.SkewedColNames),
		SkewedColumnValueLocationMaps: aws.StringMap(info.SkewedColValueLocationMaps),
		SkewedColumnValues:            aws.StringSlice(info.AWSSkewedColValues), // TODO(Guys): validate this hive uses [][]string glue uses []string (????)
	}
}

func sortColumnsLocalToGlue(columns []*metastore.Order) []*glue.Order {
	res := make([]*glue.Order, len(columns))
	for i, column := range columns {
		res[i] = &glue.Order{
			Column:    aws.String(column.Col),
			SortOrder: aws.Int64(int64(column.Order)),
		}
	}
	return res
}

func columnsLocalToGlue(columns []*metastore.FieldSchema) []*glue.Column {
	res := make([]*glue.Column, len(columns))
	for i, column := range columns {
		res[i] = &glue.Column{
			Comment: aws.String(column.Comment),
			Name:    aws.String(column.Name),
			//Parameters: nil,
			Type: aws.String(column.Type),
		}
	}
	return res
}

func DatabaseLocalToGlue(db *metastore.Database) *glue.DatabaseInput {
	return &glue.DatabaseInput{
		//CreateTableDefaultPermissions: db.,
		Description:    aws.String(db.Description),
		LocationUri:    aws.String(db.LocationURI),
		Name:           aws.String(db.Name),
		Parameters:     aws.StringMap(db.Parameters),
		TargetDatabase: db.AWSTargetDatabase,
	}
}

func DatabasesGlueToLocal(glueDatabases []*glue.Database) []*metastore.Database {
	databases := make([]*metastore.Database, len(glueDatabases))
	for i, partition := range glueDatabases {
		databases[i] = DatabaseGlueToLocal(partition)
	}
	return databases
}

func DatabaseGlueToLocal(db *glue.Database) *metastore.Database {
	return &metastore.Database{
		Name:        aws.StringValue(db.Name),
		Description: aws.StringValue(db.Description),
		LocationURI: aws.StringValue(db.LocationUri),
		Parameters:  aws.StringValueMap(db.Parameters),
	}
}

func TableLocalToGlue(table *metastore.Table) *glue.TableInput {
	sd := SDLocalToGlue(table.Sd)
	targetTable, _ := table.AWSTargetTable.(*glue.TableIdentifier)
	ht := &glue.TableInput{
		Description:       table.AWSDescription,
		LastAccessTime:    localToAWSTime(table.LastAccessTime), // TODO(Guys): check if this OK
		LastAnalyzedTime:  table.AWSLastAnalyzedTime,
		Name:              aws.String(table.TableName),
		Owner:             aws.String(table.Owner),
		Parameters:        aws.StringMap(table.Parameters),
		PartitionKeys:     columnsLocalToGlue(table.PartitionKeys),
		Retention:         aws.Int64(int64(table.Retention)), // TODO(Guys): check if this OK
		StorageDescriptor: sd,
		TableType:         aws.String(table.TableType),
		TargetTable:       targetTable,
		ViewExpandedText:  aws.String(table.ViewExpandedText),
		ViewOriginalText:  aws.String(table.ViewOriginalText),
	}
	return ht
}

func PartitionLocalToGlue(partition *metastore.Partition) *glue.PartitionInput {
	sd := SDLocalToGlue(partition.Sd)
	ht := &glue.PartitionInput{
		//IsRegisteredWithLakeFormation: partition.AWSIsRegisteredWithLakeFormation,
		LastAccessTime:    localToAWSTime(int64(partition.LastAccessTime)), // TODO(Guys): check if this OK
		LastAnalyzedTime:  partition.AWSLastAnalyzedTime,
		Parameters:        aws.StringMap(partition.Parameters),
		StorageDescriptor: sd,
		Values:            aws.StringSlice(partition.Values),
	}
	return ht
}

func PartitionsLocalToGlue(partitions []*metastore.Partition) []*glue.PartitionInput {
	gluePartitions := make([]*glue.PartitionInput, len(partitions))
	for i, partition := range partitions {
		gluePartitions[i] = PartitionLocalToGlue(partition)
	}
	return gluePartitions
}

func SDLocalToGlue(sd *metastore.StorageDescriptor) *glue.StorageDescriptor {
	if sd == nil {
		return nil
	}
	schemaRef, _ := sd.AWSSchemaReference.(*glue.SchemaReference)
	return &glue.StorageDescriptor{
		BucketColumns:          aws.StringSlice(sd.BucketCols),
		Columns:                columnsLocalToGlue(sd.Cols),
		Compressed:             aws.Bool(sd.Compressed),
		InputFormat:            aws.String(sd.InputFormat),
		Location:               aws.String(sd.Location),
		NumberOfBuckets:        aws.Int64(int64(sd.NumBuckets)),
		OutputFormat:           aws.String(sd.OutputFormat),
		Parameters:             aws.StringMap(sd.Parameters),
		SchemaReference:        schemaRef,
		SerdeInfo:              serDeLocalToGlue(sd.SerdeInfo),
		SkewedInfo:             skewedLocalToGlue(sd.SkewedInfo),
		SortColumns:            sortColumnsLocalToGlue(sd.SortCols),
		StoredAsSubDirectories: sd.StoredAsSubDirectories,
	}
}

func SDGlueToLocal(sd *glue.StorageDescriptor) *metastore.StorageDescriptor {
	if sd == nil {
		return nil
	}
	return &metastore.StorageDescriptor{
		Cols:                   columnsGlueToLocal(sd.Columns),
		Location:               aws.StringValue(sd.Location),
		InputFormat:            aws.StringValue(sd.InputFormat),
		OutputFormat:           aws.StringValue(sd.OutputFormat),
		Compressed:             aws.BoolValue(sd.Compressed),
		NumBuckets:             int(aws.Int64Value(sd.NumberOfBuckets)),
		SerdeInfo:              serDeGlueToLocal(sd.SerdeInfo),
		BucketCols:             aws.StringValueSlice(sd.BucketColumns),
		SortCols:               sortColumnsGlueToLocal(sd.SortColumns),
		Parameters:             aws.StringValueMap(sd.Parameters),
		SkewedInfo:             skewedGlueToLocal(sd.SkewedInfo),
		StoredAsSubDirectories: sd.StoredAsSubDirectories,
		AWSSchemaReference:     sd.SchemaReference,
	}
}

func localToAWSTime(t int64) *time.Time {
	res := aws.MillisecondsTimeValue(aws.Int64(t))
	return &res
}
