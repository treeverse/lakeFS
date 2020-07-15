package hive

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/metastore/mock"

	"github.com/treeverse/lakefs/metastore/hive/thrift/gen-go/hive_metastore"
)

type HiveMsMock struct {
	MockStore mock.MockStore
}

func NewHiveMsMock() *HiveMsMock {
	return &HiveMsMock{
		MockStore: mock.NewMockStore(),
	}
}

func tableToMock(table *hive_metastore.Table) *mock.MockObject {
	return &mock.MockObject{
		DbName:      table.DbName,
		TableName:   table.TableName,
		SdTableName: table.Sd.SerdeInfo.Name,
		Location:    table.Sd.Location,
		Columns:     columnsToMock(table.Sd.Cols),
	}
}

func MockToTable(mock *mock.MockObject) *hive_metastore.Table {
	return &hive_metastore.Table{
		DbName:    mock.DbName,
		TableName: mock.TableName,
		Sd: &hive_metastore.StorageDescriptor{
			Cols:      mocksToColumns(mock.Columns),
			Location:  mock.Location,
			SerdeInfo: &hive_metastore.SerDeInfo{Name: mock.SdTableName},
		},
	}
}

func partitionsToMock(partitions []*hive_metastore.Partition) []*mock.MockObject {
	var mockPartitions []*mock.MockObject
	for _, partition := range partitions {
		mockPartitions = append(mockPartitions, partitionToMock(partition))
	}
	return mockPartitions
}

func columnsToMock(columns []*hive_metastore.FieldSchema) []*mock.Column {
	var mockColumns []*mock.Column
	for _, column := range columns {
		mockColumns = append(mockColumns, &mock.Column{
			Name:    column.Name,
			Type:    column.Type,
			Comment: column.Comment,
		})
	}
	return mockColumns
}
func mocksToColumns(columns []*mock.Column) []*hive_metastore.FieldSchema {
	var mockColumns []*hive_metastore.FieldSchema
	for _, column := range columns {
		mockColumns = append(mockColumns, &hive_metastore.FieldSchema{
			Name:    column.Name,
			Type:    column.Type,
			Comment: column.Comment,
		})
	}
	return mockColumns
}

func partitionToMock(partition *hive_metastore.Partition) *mock.MockObject {
	return &mock.MockObject{
		DbName:      partition.DbName,
		TableName:   partition.TableName,
		SdTableName: partition.Sd.SerdeInfo.Name,
		Location:    partition.Sd.Location,
		Columns:     columnsToMock(partition.Sd.Cols),
		Values:      partition.Values,
	}
}

func MockToPartition(mock *mock.MockObject) *hive_metastore.Partition {
	return &hive_metastore.Partition{
		DbName:    mock.DbName,
		TableName: mock.TableName,
		Values:    mock.Values,
		Sd: &hive_metastore.StorageDescriptor{
			Cols:      mocksToColumns(mock.Columns),
			Location:  mock.Location,
			SerdeInfo: &hive_metastore.SerDeInfo{Name: mock.SdTableName},
		},
	}
}

func MockToPartitions(mockPartitions []*mock.MockObject) []*hive_metastore.Partition {
	var partitions []*hive_metastore.Partition
	for _, partition := range mockPartitions {
		partitions = append(partitions, MockToPartition(partition))
	}
	return partitions
}

func (h HiveMsMock) GetPartition(_ context.Context, db_name string, tbl_name string, part_vals []string) (r *hive_metastore.Partition, err error) {
	partition, err := h.MockStore.GetPartition(db_name, tbl_name, part_vals)
	if err != nil {
		return nil, err
	}
	return MockToPartition(partition), nil
}

func (h HiveMsMock) AlterPartition(_ context.Context, db_name string, tbl_name string, new_part *hive_metastore.Partition) (err error) {
	return h.MockStore.AlterPartition(db_name, tbl_name, partitionToMock(new_part))
}

func (h HiveMsMock) AddPartition(_ context.Context, new_part *hive_metastore.Partition) (*hive_metastore.Partition, error) {
	err := h.MockStore.AddPartition(partitionToMock(new_part))
	if err != nil {
		return nil, err
	}
	return new_part, nil
}

func (h HiveMsMock) DropPartition(_ context.Context, db_name string, tbl_name string, part_vals []string, _ bool) (r bool, err error) {
	return true, h.MockStore.DropPartition(db_name, tbl_name, part_vals)
}

func (h HiveMsMock) CreateTable(_ context.Context, tbl *hive_metastore.Table) (err error) {
	return h.MockStore.CreateTable(tbl.DbName, tbl.TableName, tableToMock(tbl))
}

func (h HiveMsMock) GetTable(_ context.Context, dbName string, tableName string) (r *hive_metastore.Table, err error) {
	mockTable, err := h.MockStore.GetTable(dbName, tableName)
	if err != nil {
		return nil, err
	}
	return MockToTable(mockTable), nil
}

func (h HiveMsMock) AlterTable(_ context.Context, dbname string, tbl_name string, table *hive_metastore.Table) (err error) {
	return h.MockStore.AlterTable(dbname, tbl_name, tableToMock(table))
}

func (h HiveMsMock) AddPartitions(_ context.Context, partitions []*hive_metastore.Partition) (r int32, err error) {
	return 0, h.MockStore.AddPartitions(partitionsToMock(partitions))
}

func (h HiveMsMock) GetPartitions(_ context.Context, dbName string, tableName string, _ int16) (r []*hive_metastore.Partition, err error) {
	partitions := h.MockStore.GetPartitions(dbName, tableName)
	return MockToPartitions(partitions), nil

}

func (h HiveMsMock) AlterPartitions(_ context.Context, dbName string, tableName string, newPartitions []*hive_metastore.Partition) (err error) {
	return h.MockStore.AlterPartitions(dbName, tableName, partitionsToMock(newPartitions))
}

func getCols() []*hive_metastore.FieldSchema {
	return []*hive_metastore.FieldSchema{
		{
			Name:    "column_one",
			Type:    "string",
			Comment: "first comment",
		},
		{
			Name:    "column_two",
			Type:    "int",
			Comment: "second comment",
		},
	}
}

func getPartitionN(db, table, location string, n int) *hive_metastore.Partition {
	partitionValue := fmt.Sprintf("part=%d", n)
	return &hive_metastore.Partition{
		Values:    []string{partitionValue},
		DbName:    db,
		TableName: table,
		Sd: &hive_metastore.StorageDescriptor{
			Cols:         getCols(),
			Location:     fmt.Sprintf("%s/%s", location, partitionValue),
			InputFormat:  "parquet.hive.DeprecatedParquetInputFormat",
			OutputFormat: "parquet.hive.DeprecatedParquetOutputFormat",

			SerdeInfo: &hive_metastore.SerDeInfo{
				Name:             table,
				SerializationLib: "",
				Parameters:       nil,
			},
		},
	}
}
func getNPartitions(db, table, location string, n int) []*hive_metastore.Partition {
	var partitions []*hive_metastore.Partition
	for i := 0; i < n; i++ {
		partitions = append(partitions, getPartitionN(db, table, location, i))
	}
	return partitions
}

func validatePartitionLocations(partitions []*hive_metastore.Partition, location string) error {
	sort.Slice(partitions, func(i, j int) bool {
		parts := strings.Split(partitions[i].GetValues()[0], "=")
		first, _ := strconv.Atoi(parts[1])
		parts = strings.Split(partitions[j].GetValues()[0], "=")
		second, _ := strconv.Atoi(parts[1])
		return first <= second
	})
	for i, partition := range partitions {
		partitionLocation := fmt.Sprintf("part=%d", i)
		expectedLocation := fmt.Sprintf("%s/%s", location, partitionLocation)
		if partition.GetSd().GetLocation() != expectedLocation {
			return fmt.Errorf("wrong partition location, expected: %s got: %s", expectedLocation, partition.GetSd().GetLocation())
		}

	}
	return nil
}

func TestMSClient_CopyAndMergeBack(t *testing.T) {
	tableName := "table"
	dbName := "default"
	repoLocation := "s3a://example"
	branch := "master"
	tableDir := "table_by_partition"
	location := fmt.Sprintf("%s/%s/%s", repoLocation, branch, tableDir)

	initialTable := &hive_metastore.Table{
		TableName: tableName,
		DbName:    dbName,
		Owner:     "test",
		Sd: &hive_metastore.StorageDescriptor{
			Cols:                   getCols(),
			Location:               location,
			InputFormat:            "",
			OutputFormat:           "",
			Compressed:             false,
			NumBuckets:             0,
			SerdeInfo:              &hive_metastore.SerDeInfo{Name: tableName},
			BucketCols:             nil,
			SortCols:               nil,
			Parameters:             nil,
			SkewedInfo:             nil,
			StoredAsSubDirectories: nil,
		},
		PartitionKeys: []*hive_metastore.FieldSchema{{
			Name:    "part",
			Type:    "int",
			Comment: "",
		}},
		Parameters:       nil,
		ViewOriginalText: "",
		ViewExpandedText: "",
		TableType:        "",
		Privileges:       nil,
		Temporary:        false,
		RewriteEnabled:   nil,
	}
	numOfPartitions := 20
	initialPartitions := getNPartitions(dbName, tableName, location, numOfPartitions)

	hiveMockClient := NewHiveMsMock()
	client := NewMetastoreClient(context.Background(), hiveMockClient)

	err := client.client.CreateTable(client.context, initialTable)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.client.AddPartitions(client.context, initialPartitions)
	if err != nil {
		t.Fatal(err)
	}
	toTableName := "copy_table"
	toDBName := "default"
	toBranch := "br1"

	err = client.CopyOrMerge(dbName, tableName, branch, toDBName, toTableName, toBranch, "")
	if err != nil {
		t.Fatal(err)
	}

	copiedTable, err := client.client.GetTable(client.context, toDBName, toTableName)
	if err != nil {
		t.Fatal(err)
	}
	expectedLocation := fmt.Sprintf("%s/%s/%s", repoLocation, toBranch, tableDir)
	if copiedTable.GetSd().GetLocation() != expectedLocation {
		t.Errorf("wrong location expected:%s got:%s", expectedLocation, copiedTable.GetSd().GetLocation())
	}

	expectedColumns := getCols()
	gotColumns := copiedTable.GetSd().GetCols()
	for i, expectedColumn := range expectedColumns {
		if expectedColumn.Name != gotColumns[i].Name {
			t.Errorf("wrong column expected:%s got:%s ", expectedColumn.Name, gotColumns[i].Name)
		}
		if !FieldSchemaEqual(expectedColumn, gotColumns[i]) {
			t.Fatalf("wrong column data for column %s", expectedColumn.Name)
		}
	}

	if copiedTable.GetSd().GetSerdeInfo().Name != toTableName {
		t.Fatalf("wrong serde info name. expected: %s, got:%s", toTableName, copiedTable.GetSd().GetSerdeInfo().Name)
	}

	copiedPartitions, err := client.client.GetPartitions(client.context, toDBName, toTableName, 1000)
	//compare partitions
	if len(copiedPartitions) != numOfPartitions {
		t.Fatalf("got wrong amount of partitions expected:%d, got:%d", numOfPartitions, len(copiedPartitions))
	}

	err = validatePartitionLocations(copiedPartitions, expectedLocation)
	if err != nil {
		t.Fatal(err)
	}

	//verify first partition (enough)

	firstPartition := copiedPartitions[0]

	if firstPartition.GetSd().GetSerdeInfo().Name != toTableName {
		t.Fatalf("wrong serde info name. expected: %s, got: %s", toTableName, firstPartition.GetSd().GetSerdeInfo().Name)
	}
	gotColumns = firstPartition.GetSd().GetCols()
	for i, expectedColumn := range expectedColumns {
		if expectedColumn.Name != gotColumns[i].Name {
			t.Errorf("wrong column expected:%s got:%s ", expectedColumn.Name, gotColumns[i].Name)
		}
		if !FieldSchemaEqual(expectedColumn, gotColumns[i]) {
			t.Fatalf("wrong column data for column %s", expectedColumn.Name)
		}
	}

	partition17 := copiedPartitions[17]
	_, err = client.client.DropPartition(client.context, toDBName, toTableName, partition17.Values, true)
	if err != nil {
		t.Fatal(err)
	}
	//add columns to existing partition
	partition19 := copiedPartitions[19]
	addColumn := &hive_metastore.FieldSchema{
		Name:    "column_three",
		Type:    "string",
		Comment: "added column before merge",
	}
	partition19.Sd.Cols = append(partition19.Sd.Cols, addColumn)
	// add partition
	partitionNum := len(copiedPartitions)
	newPartition := getPartitionN(toDBName, toTableName, expectedLocation, partitionNum)
	// add column
	newPartition.GetSd().Cols = append(newPartition.GetSd().Cols, addColumn)

	_, err = client.client.AddPartitions(client.context, []*hive_metastore.Partition{newPartition})
	if err != nil {
		t.Fatal(err)
	}

	err = client.client.AlterPartitions(client.context, toDBName, toTableName, []*hive_metastore.Partition{partition19})
	if err != nil {
		t.Fatal(err)
	}
	// now merge back
	err = client.CopyOrMerge(toDBName, toTableName, toBranch, dbName, tableName, branch, "")
	if err != nil {
		t.Fatal(err)
	}

	merged, err := client.client.GetTable(client.context, dbName, tableName)
	mergedPartitions, err := client.client.GetPartitions(client.context, dbName, tableName, 1000)

	if merged.Sd.GetLocation() != location {
		t.Errorf("wrong location expected:%s got:%s", location, merged.Sd.GetLocation())

	}

	if len(mergedPartitions) != numOfPartitions {
		t.Fatalf("got wrong amount of partitions expected:%d, got:%d", numOfPartitions, len(mergedPartitions))
	}

	// check if partition 20 was added and has three columns, 19 was updated and 17 was deleted
	partition19 = nil
	partition17 = nil
	var partition20 *hive_metastore.Partition
	for _, partition := range mergedPartitions {
		if partition.GetValues()[0] == "part=20" {
			partition20 = partition
		}
		if partition.GetValues()[0] == "part=19" {
			partition19 = partition
		}
		if partition.GetValues()[0] == "part=17" {
			partition17 = partition
		}
	}
	if partition20 == nil {
		t.Fatalf("expected to have a partition with value: part=20 after merge")
	}
	for _, mergedColumns := range [][]*hive_metastore.FieldSchema{partition19.Sd.Cols, partition20.Sd.Cols} {
		if len(mergedColumns) != 3 {
			t.Fatal("expected added partition to have 3 columns")
		}
		if mergedColumns[2].Name != "column_three" {
			t.Fatalf("expected added column to be column_three got:%s", mergedColumns[2].Name)
		}
	}

}
