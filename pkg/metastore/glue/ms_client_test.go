package glue

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/pkg/testutil"

	"github.com/treeverse/lakefs/pkg/metastore/mock"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
)

type GlueMsMock struct {
	glueiface.GlueAPI
	MockStore mock.MetaStore
}

func NewGlueMsMock() *GlueMsMock {
	return &GlueMsMock{
		MockStore: mock.NewMockStore(),
	}
}

func tableToMock(db *string, table *glue.TableInput) *mock.MetastoreObject {
	return &mock.MetastoreObject{
		DBName:      aws.StringValue(db),
		TableName:   aws.StringValue(table.Name),
		SdTableName: aws.StringValue(table.StorageDescriptor.SerdeInfo.Name),
		Location:    aws.StringValue(table.StorageDescriptor.Location),
		Columns:     columnsToMock(table.StorageDescriptor.Columns),
	}
}

func MockToTable(mock *mock.MetastoreObject) *glue.TableData {
	return &glue.TableData{
		DatabaseName: aws.String(mock.DBName),
		Name:         aws.String(mock.TableName),
		StorageDescriptor: &glue.StorageDescriptor{
			Columns:   mocksToColumns(mock.Columns),
			Location:  aws.String(mock.Location),
			SerdeInfo: &glue.SerDeInfo{Name: aws.String(mock.SdTableName)},
		},
	}
}

func partitionsToMock(db, table *string, partitions []*glue.PartitionInput) []*mock.MetastoreObject {
	var mockPartitions []*mock.MetastoreObject
	for _, partition := range partitions {
		mockPartitions = append(mockPartitions, partitionToMock(db, table, partition))
	}
	return mockPartitions
}

func columnsToMock(columns []*glue.Column) []*mock.Column {
	var mockColumns []*mock.Column
	for _, column := range columns {
		mockColumns = append(mockColumns, &mock.Column{
			Name:    aws.StringValue(column.Name),
			Type:    aws.StringValue(column.Type),
			Comment: aws.StringValue(column.Comment),
		})
	}
	return mockColumns
}

func mocksToColumns(columns []*mock.Column) []*glue.Column {
	var mockColumns []*glue.Column
	for _, column := range columns {
		mockColumns = append(mockColumns, &glue.Column{
			Name:    aws.String(column.Name),
			Type:    aws.String(column.Type),
			Comment: aws.String(column.Comment),
		})
	}
	return mockColumns
}

func partitionInputToMock(db, table *string, partition *glue.PartitionInput) *mock.MetastoreObject {
	return &mock.MetastoreObject{
		DBName:      aws.StringValue(db),
		TableName:   aws.StringValue(table),
		SdTableName: aws.StringValue(partition.StorageDescriptor.SerdeInfo.Name),
		Location:    aws.StringValue(partition.StorageDescriptor.Location),
		Columns:     columnsToMock(partition.StorageDescriptor.Columns),
		Values:      aws.StringValueSlice(partition.Values),
	}
}

func partitionToMock(db, table *string, partition *glue.PartitionInput) *mock.MetastoreObject {
	return &mock.MetastoreObject{
		DBName:      aws.StringValue(db),
		TableName:   aws.StringValue(table),
		SdTableName: aws.StringValue(partition.StorageDescriptor.SerdeInfo.Name),
		Location:    aws.StringValue(partition.StorageDescriptor.Location),
		Columns:     columnsToMock(partition.StorageDescriptor.Columns),
		Values:      aws.StringValueSlice(partition.Values),
	}
}

func MockToPartition(mock *mock.MetastoreObject) *glue.Partition {
	return &glue.Partition{
		DatabaseName: aws.String(mock.DBName),
		TableName:    aws.String(mock.TableName),
		Values:       aws.StringSlice(mock.Values),
		StorageDescriptor: &glue.StorageDescriptor{
			Columns:   mocksToColumns(mock.Columns),
			Location:  aws.String(mock.Location),
			SerdeInfo: &glue.SerDeInfo{Name: aws.String(mock.SdTableName)},
		},
	}
}

func MockToPartitions(mockPartitions []*mock.MetastoreObject) []*glue.Partition {
	var partitions []*glue.Partition
	for _, partition := range mockPartitions {
		partitions = append(partitions, MockToPartition(partition))
	}
	return partitions
}

func (h GlueMsMock) GetPartition(partitionInput *glue.GetPartitionInput) (*glue.GetPartitionOutput, error) {
	partition, err := h.MockStore.GetPartition(aws.StringValue(partitionInput.DatabaseName), aws.StringValue(partitionInput.TableName), aws.StringValueSlice(partitionInput.PartitionValues))
	if err != nil {
		if errors.Is(err, mock.ErrNotFound) {
			return nil, &glue.EntityNotFoundException{Message_: aws.String(err.Error())}
		}
		return nil, err
	}
	return &glue.GetPartitionOutput{Partition: MockToPartition(partition)}, nil
}

func (h GlueMsMock) UpdatePartition(input *glue.UpdatePartitionInput) (*glue.UpdatePartitionOutput, error) {
	return nil, h.MockStore.AlterPartition(aws.StringValue(input.DatabaseName), aws.StringValue(input.TableName), partitionInputToMock(input.DatabaseName, input.TableName, input.PartitionInput))
}

func (h GlueMsMock) CreatePartition(partition *glue.CreatePartitionInput) (*glue.CreatePartitionOutput, error) {
	return nil, h.MockStore.AddPartition(partitionInputToMock(partition.DatabaseName, partition.TableName, partition.PartitionInput))
}

func (h GlueMsMock) DeletePartition(input *glue.DeletePartitionInput) (*glue.DeletePartitionOutput, error) {
	return nil, h.MockStore.DropPartition(aws.StringValue(input.DatabaseName), aws.StringValue(input.TableName), aws.StringValueSlice(input.PartitionValues))
}

func (h GlueMsMock) CreateTable(tbl *glue.CreateTableInput) (*glue.CreateTableOutput, error) {
	return nil, h.MockStore.CreateTable(aws.StringValue(tbl.DatabaseName), aws.StringValue(tbl.TableInput.Name), tableToMock(tbl.DatabaseName, tbl.TableInput))
}

func (h GlueMsMock) GetTable(input *glue.GetTableInput) (*glue.GetTableOutput, error) {
	mockTable, err := h.MockStore.GetTable(aws.StringValue(input.DatabaseName), aws.StringValue(input.Name))
	if err != nil {
		if errors.Is(err, mock.ErrNotFound) {
			return nil, &glue.EntityNotFoundException{Message_: aws.String(err.Error())}
		}
		return nil, err
	}
	return &glue.GetTableOutput{Table: MockToTable(mockTable)}, nil
}

func (h GlueMsMock) BatchDeletePartition(input *glue.BatchDeletePartitionInput) (*glue.BatchDeletePartitionOutput, error) {
	for _, partition := range input.PartitionsToDelete {
		err := h.MockStore.DropPartition(aws.StringValue(input.DatabaseName), aws.StringValue(input.TableName), aws.StringValueSlice(partition.Values))
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (h GlueMsMock) UpdateTable(input *glue.UpdateTableInput) (*glue.UpdateTableOutput, error) {
	return nil, h.MockStore.AlterTable(aws.StringValue(input.DatabaseName), aws.StringValue(input.TableInput.Name), tableToMock(input.DatabaseName, input.TableInput))
}

func (h GlueMsMock) BatchCreatePartition(input *glue.BatchCreatePartitionInput) (*glue.BatchCreatePartitionOutput, error) {
	return nil, h.MockStore.AddPartitions(partitionsToMock(input.DatabaseName, input.TableName, input.PartitionInputList))
}

func (h GlueMsMock) GetPartitions(input *glue.GetPartitionsInput) (*glue.GetPartitionsOutput, error) {
	partitions := h.MockStore.GetPartitions(aws.StringValue(input.DatabaseName), aws.StringValue(input.TableName))
	return &glue.GetPartitionsOutput{
		Partitions: MockToPartitions(partitions),
	}, nil
}

func getCols() []*glue.Column {
	return []*glue.Column{
		{
			Name:    aws.String("column_one"),
			Type:    aws.String("string"),
			Comment: aws.String("first comment"),
		},
		{
			Name:    aws.String("column_two"),
			Type:    aws.String("int"),
			Comment: aws.String("second comment"),
		},
	}
}

func getPartitionN(table, location string, n int) *glue.Partition {
	partitionValue := fmt.Sprintf("part=%d", n)
	return &glue.Partition{
		Values: aws.StringSlice([]string{partitionValue}),
		StorageDescriptor: &glue.StorageDescriptor{
			Columns:      getCols(),
			Location:     aws.String(fmt.Sprintf("%s/%s", location, partitionValue)),
			InputFormat:  aws.String("parquet.hive.DeprecatedParquetInputFormat"),
			OutputFormat: aws.String("parquet.hive.DeprecatedParquetOutputFormat"),

			SerdeInfo: &glue.SerDeInfo{
				Name: aws.String(table),
			},
		},
	}
}

func getNPartitions(table, location string, n int) []*glue.Partition {
	var partitions []*glue.Partition
	for i := 0; i < n; i++ {
		partitions = append(partitions, getPartitionN(table, location, i))
	}
	return partitions
}

func validatePartitionLocations(partitions []*glue.Partition, location string) error {
	sort.Slice(partitions, func(i, j int) bool {
		parts := strings.Split(aws.StringValue(partitions[i].Values[0]), "=")
		first, _ := strconv.Atoi(parts[1])
		parts = strings.Split(aws.StringValue(partitions[j].Values[0]), "=")
		second, _ := strconv.Atoi(parts[1])
		return first <= second
	})
	for i, partition := range partitions {
		partitionLocation := fmt.Sprintf("part=%d", i)
		expectedLocation := fmt.Sprintf("%s/%s", location, partitionLocation)
		gotLocation := aws.StringValue(partition.StorageDescriptor.Location)
		if gotLocation != expectedLocation {
			return fmt.Errorf("%w, expected: %s got: %s", ErrWrongPartitionLocation, expectedLocation, gotLocation)
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

	initialTable := &glue.TableData{
		DatabaseName: aws.String(dbName),
		Name:         aws.String(tableName),
		StorageDescriptor: &glue.StorageDescriptor{
			Columns:   getCols(),
			Location:  aws.String(location),
			SerdeInfo: &glue.SerDeInfo{Name: aws.String(tableName)},
		},
		PartitionKeys: []*glue.Column{{
			Name:    aws.String("part"),
			Type:    aws.String("int"),
			Comment: nil,
		}},
	}
	numOfPartitions := 20
	initialPartitions := getNPartitions(tableName, location, numOfPartitions)

	client := &MSClient{
		client:    NewGlueMsMock(),
		catalogID: "",
	}

	err := client.createTable(dbName, initialTable)
	if err != nil {
		t.Fatal(err)
	}
	err = client.addPartitions(dbName, tableName, initialPartitions)
	if err != nil {
		t.Fatal(err)
	}
	toTableName := "copy_table"
	toDBName := "default"
	toBranch := "br1"

	err = client.CopyOrMerge(dbName, tableName, toDBName, toTableName, toBranch, toTableName, nil)
	if err != nil {
		t.Fatal(err)
	}
	copiedTable, err := client.getTable(toDBName, toTableName)
	if err != nil {
		t.Fatal(err)
	}
	expectedLocation := fmt.Sprintf("%s/%s/%s", repoLocation, toBranch, tableDir)
	gotLocation := aws.StringValue(copiedTable.StorageDescriptor.Location)
	if gotLocation != expectedLocation {
		t.Errorf("%w:%s got:%s", ErrWrongLocationExpected, expectedLocation, gotLocation)
	}

	expectedColumns := getCols()
	gotColumns := copiedTable.StorageDescriptor.Columns
	for i, expectedColumn := range expectedColumns {
		if aws.StringValue(expectedColumn.Name) != aws.StringValue(gotColumns[i].Name) {
			t.Errorf("%w:%s got:%s ", ErrWrongColumnExpected, aws.StringValue(expectedColumn.Name), aws.StringValue(gotColumns[i].Name))
		}
		if !ColumnEqual(expectedColumn, gotColumns[i]) {
			t.Fatalf("wrong column data for column %s", aws.StringValue(expectedColumn.Name))
		}
	}

	gotSerdeName := aws.StringValue(copiedTable.StorageDescriptor.SerdeInfo.Name)
	if gotSerdeName != toTableName {
		t.Fatalf("wrong serde info name. expected: %s, got:%s", toTableName, gotSerdeName)
	}

	copiedPartitions, err := client.getAllPartitions(toDBName, toTableName)
	testutil.Must(t, err)
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
	firstPartitionName := aws.StringValue(firstPartition.StorageDescriptor.SerdeInfo.Name)
	if firstPartitionName != toTableName {
		t.Fatalf("wrong serde info name. expected: %s, got: %s", toTableName, firstPartitionName)
	}
	gotColumns = firstPartition.StorageDescriptor.Columns
	for i, expectedColumn := range expectedColumns {
		if aws.StringValue(expectedColumn.Name) != aws.StringValue(gotColumns[i].Name) {
			t.Errorf("%w:%s got:%s ", ErrWrongColumnExpected, aws.StringValue(expectedColumn.Name), aws.StringValue(gotColumns[i].Name))
		}
		if !ColumnEqual(expectedColumn, gotColumns[i]) {
			t.Fatalf("wrong column data for column %s", aws.StringValue(expectedColumn.Name))
		}
	}

	// drop partition 17
	partition17 := copiedPartitions[17]
	err = client.removePartitions(toDBName, toTableName, []*glue.Partition{partition17})
	if err != nil {
		t.Fatal(err)
	}

	//add columns to existing partition
	partition19 := copiedPartitions[19]
	addColumn := &glue.Column{
		Name:    aws.String("column_three"),
		Type:    aws.String("string"),
		Comment: aws.String("added column before merge"),
	}
	partition19.StorageDescriptor.Columns = append(partition19.StorageDescriptor.Columns, addColumn)
	// add partition
	partition20 := getPartitionN(toTableName, expectedLocation, 20)
	// add column
	partition20.StorageDescriptor.Columns = append(partition20.StorageDescriptor.Columns, addColumn)

	err = client.addPartitions(toDBName, toTableName, []*glue.Partition{partition20})
	if err != nil {
		t.Fatal(err)
	}

	err = client.alterPartitions(toDBName, toTableName, []*glue.Partition{partition19})
	if err != nil {
		t.Fatal(err)
	}
	// now merge back
	err = client.CopyOrMerge(toDBName, toTableName, dbName, tableName, branch, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	merged, err := client.getTable(dbName, tableName)
	testutil.Must(t, err)
	mergedPartitions, err := client.getAllPartitions(dbName, tableName)
	testutil.Must(t, err)

	mergedSdLocation := aws.StringValue(merged.StorageDescriptor.Location)
	if mergedSdLocation != location {
		t.Errorf("%w:%s got:%s", ErrWrongLocationExpected, location, mergedSdLocation)
	}

	if len(mergedPartitions) != numOfPartitions {
		t.Fatalf("got wrong amount of partitions expected:%d, got:%d", numOfPartitions+1, len(mergedPartitions))
	}

	// check if partition 20 was added and has three columns, 19 was updated and 17 was deleted
	m := make(map[string]*glue.Partition)
	for _, partition := range mergedPartitions {
		k := aws.StringValue(partition.Values[0])
		m[k] = partition
	}
	existingPartitions := []string{"part=19", "part=20"}
	for _, p := range existingPartitions {
		// check partition exists
		partition, ok := m[p]
		if !ok {
			t.Fatalf("expected to have a partition with value: %s after merge", p)
		}
		// check that columns are added
		columns := partition.StorageDescriptor.Columns
		if len(columns) != 3 {
			t.Fatalf("expected added partition (%s) to have 3 columns, found %d", p, len(columns))
		}
		columnName := aws.StringValue(columns[2].Name)
		if columnName != "column_three" {
			t.Fatalf("expected added column to be column_three got: %s", columnName)
		}
	}
	if _, ok := m["part=17"]; ok {
		t.Fatal("expected part=17 partition to be deleted")
	}
}
