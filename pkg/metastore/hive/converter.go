package hive

import (
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/hive/gen-go/hive_metastore"
)

func serDeHiveToLocal(g *hive_metastore.SerDeInfo) *metastore.SerDeInfo {
	if g == nil {
		return nil
	}
	return &metastore.SerDeInfo{
		Name:             g.Name,
		SerializationLib: g.SerializationLib,
		Parameters:       g.Parameters,
	}
}

func skewedHiveToLocal(g *hive_metastore.SkewedInfo) *metastore.SkewedInfo {
	if g == nil {
		return &metastore.SkewedInfo{}
	}
	return &metastore.SkewedInfo{
		SkewedColNames:             g.SkewedColNames,
		SkewedColValues:            g.SkewedColValues,
		SkewedColValueLocationMaps: g.SkewedColValueLocationMaps,
	}
}

func sortColumnsHiveToLocal(columns []*hive_metastore.Order) []*metastore.Order {
	res := make([]*metastore.Order, len(columns))
	for i, column := range columns {
		res[i] = &metastore.Order{
			Col:   column.Col,
			Order: int(column.Order),
		}
	}
	return res
}

func columnsHiveToLocal(columns []*hive_metastore.FieldSchema) []*metastore.FieldSchema {
	res := make([]*metastore.FieldSchema, len(columns))
	for i, column := range columns {
		res[i] = &metastore.FieldSchema{
			Name:    column.Name,
			Type:    column.Type,
			Comment: column.Comment,
		}
	}
	return res
}

func TableHiveToLocal(hiveTable *hive_metastore.Table) *metastore.Table {
	sd := SDHiveToLocal(hiveTable.Sd)
	ht := &metastore.Table{
		TableName:        hiveTable.TableName,
		DBName:           hiveTable.DbName,
		Owner:            hiveTable.Owner,
		CreateTime:       int64(hiveTable.CreateTime),
		LastAccessTime:   int64(hiveTable.LastAccessTime),
		Retention:        int(hiveTable.Retention),
		Sd:               sd,
		PartitionKeys:    columnsHiveToLocal(hiveTable.PartitionKeys),
		Parameters:       hiveTable.Parameters,
		ViewOriginalText: hiveTable.ViewOriginalText,
		ViewExpandedText: hiveTable.ViewExpandedText,
		TableType:        hiveTable.TableType,
		Temporary:        hiveTable.Temporary,
		RewriteEnabled:   hiveTable.RewriteEnabled,
	}
	return ht
}

func serDeLocalToHive(info *metastore.SerDeInfo) *hive_metastore.SerDeInfo {
	if info == nil {
		return nil
	}
	return &hive_metastore.SerDeInfo{
		Name:             info.Name,
		SerializationLib: info.SerializationLib,
		Parameters:       info.Parameters,
	}
}

func skewedLocalToHive(info *metastore.SkewedInfo) *hive_metastore.SkewedInfo {
	if info == nil {
		return &hive_metastore.SkewedInfo{}
	}
	return &hive_metastore.SkewedInfo{
		SkewedColNames:             info.SkewedColNames,
		SkewedColValues:            info.SkewedColValues,
		SkewedColValueLocationMaps: info.SkewedColValueLocationMaps,
	}
}

func sortColumnsLocalToHive(columns []*metastore.Order) []*hive_metastore.Order {
	res := make([]*hive_metastore.Order, len(columns))
	for i, column := range columns {
		res[i] = &hive_metastore.Order{
			Col:   column.Col,
			Order: int32(column.Order), //nolint:gosec
		}
	}
	return res
}

func columnsLocalToHive(columns []*metastore.FieldSchema) []*hive_metastore.FieldSchema {
	res := make([]*hive_metastore.FieldSchema, len(columns))
	for i, column := range columns {
		res[i] = &hive_metastore.FieldSchema{
			Name:    column.Name,
			Type:    column.Type,
			Comment: column.Comment,
		}
	}
	return res
}

func TableLocalToHive(table *metastore.Table) *hive_metastore.Table {
	sd := SDLocalToHive(table.Sd)
	privileges, _ := table.Privileges.(*hive_metastore.PrincipalPrivilegeSet)

	ht := &hive_metastore.Table{
		DbName:         table.DBName,
		TableName:      table.TableName,
		Owner:          table.Owner,
		CreateTime:     int32(table.CreateTime),     //nolint:gosec
		LastAccessTime: int32(table.LastAccessTime), //nolint:gosec
		Retention:      int32(table.Retention),      //nolint:gosec
		Sd:             sd,

		PartitionKeys:    columnsLocalToHive(table.PartitionKeys),
		Parameters:       table.Parameters,
		ViewOriginalText: table.ViewOriginalText,
		ViewExpandedText: table.ViewExpandedText,
		TableType:        table.TableType,
		Privileges:       privileges,
		Temporary:        table.Temporary,
		RewriteEnabled:   table.RewriteEnabled,
	}
	return ht
}

func PartitionHiveToLocal(hivePartition *hive_metastore.Partition) *metastore.Partition {
	sd := SDHiveToLocal(hivePartition.Sd)
	partition := &metastore.Partition{
		Values:         hivePartition.Values,
		DBName:         hivePartition.DbName,
		TableName:      hivePartition.TableName,
		CreateTime:     int(hivePartition.CreateTime),
		LastAccessTime: int(hivePartition.LastAccessTime),
		Sd:             sd,
		Parameters:     hivePartition.Parameters,
	}
	return partition
}
func PartitionsHiveToLocal(gluePartitions []*hive_metastore.Partition) []*metastore.Partition {
	partitions := make([]*metastore.Partition, len(gluePartitions))
	for i, partition := range gluePartitions {
		partitions[i] = PartitionHiveToLocal(partition)
	}
	return partitions
}

func DatabaseLocalToHive(db *metastore.Database) *hive_metastore.Database {
	privileges, _ := db.HivePrivileges.(*hive_metastore.PrincipalPrivilegeSet)
	ownerType, _ := db.HiveOwnerType.(*hive_metastore.PrincipalType)
	return &hive_metastore.Database{
		Name:        db.Name,
		Description: db.Description,
		LocationUri: db.LocationURI,
		Parameters:  db.Parameters,
		Privileges:  privileges,
		OwnerName:   db.OwnerName,
		OwnerType:   ownerType,
	}
}

func DatabaseHiveToLocal(db *hive_metastore.Database) *metastore.Database {
	return &metastore.Database{
		Name:        db.Name,
		Description: db.Description,
		LocationURI: db.LocationUri,
		Parameters:  db.Parameters,
	}
}

func PartitionLocalToHive(partition *metastore.Partition) *hive_metastore.Partition {
	sd := SDLocalToHive(partition.Sd)
	privileges, _ := partition.Privileges.(*hive_metastore.PrincipalPrivilegeSet)
	ht := &hive_metastore.Partition{
		Values:         partition.Values,
		DbName:         partition.DBName,
		TableName:      partition.TableName,
		CreateTime:     int32(partition.CreateTime),     //nolint:gosec
		LastAccessTime: int32(partition.LastAccessTime), //nolint:gosec
		Sd:             sd,
		Parameters:     partition.Parameters,
		Privileges:     privileges,
	}
	return ht
}

func SDLocalToHive(sd *metastore.StorageDescriptor) *hive_metastore.StorageDescriptor {
	if sd == nil {
		return nil
	}
	return &hive_metastore.StorageDescriptor{
		Cols:                   columnsLocalToHive(sd.Cols),
		Location:               sd.Location,
		InputFormat:            sd.InputFormat,
		OutputFormat:           sd.OutputFormat,
		Compressed:             sd.Compressed,
		NumBuckets:             int32(sd.NumBuckets), //nolint:gosec
		SerdeInfo:              serDeLocalToHive(sd.SerdeInfo),
		BucketCols:             sd.BucketCols,
		SortCols:               sortColumnsLocalToHive(sd.SortCols),
		Parameters:             sd.Parameters,
		SkewedInfo:             skewedLocalToHive(sd.SkewedInfo),
		StoredAsSubDirectories: sd.StoredAsSubDirectories,
	}
}

func SDHiveToLocal(sd *hive_metastore.StorageDescriptor) *metastore.StorageDescriptor {
	if sd == nil {
		return nil
	}
	return &metastore.StorageDescriptor{
		Cols:                   columnsHiveToLocal(sd.Cols),
		Location:               sd.Location,
		InputFormat:            sd.InputFormat,
		OutputFormat:           sd.OutputFormat,
		Compressed:             sd.Compressed,
		NumBuckets:             int(sd.NumBuckets),
		SerdeInfo:              serDeHiveToLocal(sd.SerdeInfo),
		BucketCols:             sd.BucketCols,
		SortCols:               sortColumnsHiveToLocal(sd.SortCols),
		Parameters:             sd.Parameters,
		SkewedInfo:             skewedHiveToLocal(sd.SkewedInfo),
		StoredAsSubDirectories: sd.StoredAsSubDirectories,
	}
}
func PartitionsLocalToHive(partitions []*metastore.Partition) []*hive_metastore.Partition {
	gluePartitions := make([]*hive_metastore.Partition, len(partitions))
	for i, partition := range partitions {
		gluePartitions[i] = PartitionLocalToHive(partition)
	}
	return gluePartitions
}
