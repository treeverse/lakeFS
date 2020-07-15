package glueClient

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/metastore/mock"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
)

//func getService() *glue.Glue {
//	cfg := &aws.Config{
//		Region: aws.String("us-east-1"),
//		//Logger: &config.LogrusAWSAdapter{},
//	}
//	cfg.Credentials = credentials.NewStaticCredentials(
//		"[clientId]",
//		"[secret]]",
//		"")
//
//	sess := session.Must(session.NewSession(cfg))
//	sess.ClientConfig("glue")
//
//	return glue.New(sess)
//}
//func TestGlueAddTable(t *testing.T) {
//	dbName := "default"
//	tableName := "lakefs-sc_by_dt"
//	columns := []*glue.Column{
//		{
//			Name: aws.String("iso_code"),
//			Type: aws.String("string"),
//		},
//		{
//			Name: aws.String("location"),
//			Type: aws.String("string"),
//		},
//		{
//			Name: aws.String("total_cases"),
//			Type: aws.String("binary"),
//		},
//	}
//
//	serde := &glue.SerDeInfo{
//		Name:                 aws.String(tableName),
//		Parameters:           map[string]*string{"serialization.format": aws.String("1")},
//		SerializationLibrary: aws.String("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
//	}
//	inputFormat := aws.String("parquet.hive.DeprecatedParquetInputFormat")
//	outputFormat := aws.String("parquet.hive.DeprecatedParquetOutputFormat")
//	location := "s3a://example-2/master/sc_by_date/"
//	g := &GlueMSClient{
//		client:    getService(),
//		catalogID: aws.String("977611293394"),
//	}
//
//	err := g.createTable(dbName, &glue.TableData{
//		CreatedBy:    aws.String("lakefs-test"),
//		DatabaseName: aws.String(dbName),
//		Name:         aws.String(tableName),
//		Owner:        aws.String("lakefs-test"),
//		PartitionKeys: []*glue.Column{{
//			Name: aws.String("dt"),
//			Type: aws.String("string"),
//		}},
//		StorageDescriptor: &glue.StorageDescriptor{
//			Columns:      columns,
//			InputFormat:  inputFormat,
//			Location:     aws.String(location),
//			OutputFormat: outputFormat,
//			SerdeInfo:    serde,
//		},
//		TableType: aws.String("parquet"),
//	})
//	if err != nil {
//		t.Fatal(err)
//	}
//	var partitions []*glue.Partition
//
//	for i := 1; i < 31; i++ {
//		value := fmt.Sprintf("2020-04-%02d", i)
//		partitions = append(partitions, &glue.Partition{
//			DatabaseName: aws.String(dbName),
//			StorageDescriptor: &glue.StorageDescriptor{
//				Columns:      columns,
//				InputFormat:  inputFormat,
//				Location:     aws.String(fmt.Sprintf("%sdate=%s", location, value)),
//				OutputFormat: outputFormat,
//				SerdeInfo:    serde,
//			},
//			TableName: nil,
//			Values:    []*string{aws.String(value)},
//		})
//	}
//	g.addPartitions(dbName, tableName, partitions)
//	if err != nil {
//		t.Fatal(err)
//	}
//}
//func TestGlueMSClient_AddPartitions(t *testing.T) {
//	type fields struct {
//		Svc       glueiface.GlueAPI
//		CatalogID *string
//	}
//	type args struct {
//		dbName     string
//		tableName  string
//		partitions []*glue.Partition
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		wantErr bool
//	}{
//		{
//			name: "add partitions",
//			fields: fields{
//				Svc:       getService(),
//				CatalogID: aws.String("977611293394"),
//			},
//			args: args{
//				dbName:    "default",
//				tableName: "imdb_nice_date_2",
//				partitions: []*glue.Partition{
//					{
//						//CreationTime:      nil,
//						DatabaseName: aws.String("default"),
//						//LastAccessTime:    nil,
//						//LastAnalyzedTime:  nil,
//						//Parameters:        nil,
//						StorageDescriptor: &glue.StorageDescriptor{
//							Location: aws.String("s3a://example/br1/collection/shows/titles_by_year/startYear=2017"),
//							SerdeInfo: &glue.SerDeInfo{
//								Name: aws.String("imdb_nice_date_2"),
//							},
//						},
//						TableName: aws.String("imdb_nice_date_2"),
//						Values:    []*string{aws.String("2017")},
//					},
//					{
//						//CreationTime:      nil,
//						DatabaseName: aws.String("default"),
//						//LastAccessTime:    nil,
//						//LastAnalyzedTime:  nil,
//						//Parameters:        nil,
//						StorageDescriptor: &glue.StorageDescriptor{
//							Location: aws.String("s3a://example/br1/collection/shows/titles_by_year/startYear=2018"),
//							SerdeInfo: &glue.SerDeInfo{
//								Name: aws.String("imdb_nice_date_2"),
//							},
//						},
//						TableName: aws.String("imdb_nice_date_2"),
//						Values:    []*string{aws.String("2018")},
//					},
//				},
//			},
//			wantErr: false,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			g := &GlueMSClient{
//				client:    tt.fields.Svc,
//				catalogID: tt.fields.CatalogID,
//			}
//			if err := g.addPartitions(tt.args.dbName, tt.args.tableName, tt.args.partitions); (err != nil) != tt.wantErr {
//				t.Errorf("addPartitions() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//func TestAlterPartition(t *testing.T) {
//	g := &GlueMSClient{
//		client:    getService(),
//		catalogID: aws.String("977611293394"),
//	}
//
//	partitions, err := g.getAllPartitions("default", "imdb_nice_date_2")
//	if err != nil {
//		t.Error(err)
//	}
//	partition := partitions[1]
//	fmt.Printf("working on partition %s", aws.StringValue(partition.Values[0]))
//	mp := make(map[string]*string)
//	mp["what"] = aws.String("OK")
//	partition.Parameters = mp
//	np := []*glue.Partition{partition}
//	err = g.alterPartitions("default", "imdb_nice_date_2", np)
//	if err != nil {
//		fmt.Printf(err.Error())
//	}
//	fmt.Print("done")
//}

type GlueMsMock struct {
	glueiface.GlueAPI
	MockStore mock.MockStore
}

func NewGlueMsMock() *GlueMsMock {
	return &GlueMsMock{
		MockStore: mock.NewMockStore(),
	}
}

func tableToMock(db *string, table *glue.TableInput) *mock.MockObject {
	return &mock.MockObject{
		DbName:      aws.StringValue(db),
		TableName:   aws.StringValue(table.Name),
		SdTableName: aws.StringValue(table.StorageDescriptor.SerdeInfo.Name),
		Location:    aws.StringValue(table.StorageDescriptor.Location),
		Columns:     columnsToMock(table.StorageDescriptor.Columns),
	}
}

func MockToTable(mock *mock.MockObject) *glue.TableData {
	return &glue.TableData{
		DatabaseName: aws.String(mock.DbName),
		Name:         aws.String(mock.TableName),
		StorageDescriptor: &glue.StorageDescriptor{
			Columns:   mocksToColumns(mock.Columns),
			Location:  aws.String(mock.Location),
			SerdeInfo: &glue.SerDeInfo{Name: aws.String(mock.SdTableName)},
		},
	}
}

func partitionsToMock(db, table *string, partitions []*glue.PartitionInput) []*mock.MockObject {
	var mockPartitions []*mock.MockObject
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

func partitionInputToMock(db, table *string, partition *glue.PartitionInput) *mock.MockObject {
	return &mock.MockObject{
		DbName:      aws.StringValue(db),
		TableName:   aws.StringValue(table),
		SdTableName: aws.StringValue(partition.StorageDescriptor.SerdeInfo.Name),
		Location:    aws.StringValue(partition.StorageDescriptor.Location),
		Columns:     columnsToMock(partition.StorageDescriptor.Columns),
		Values:      aws.StringValueSlice(partition.Values),
	}
}
func partitionToMock(db, table *string, partition *glue.PartitionInput) *mock.MockObject {
	return &mock.MockObject{
		DbName:      aws.StringValue(db),
		TableName:   aws.StringValue(table),
		SdTableName: aws.StringValue(partition.StorageDescriptor.SerdeInfo.Name),
		Location:    aws.StringValue(partition.StorageDescriptor.Location),
		Columns:     columnsToMock(partition.StorageDescriptor.Columns),
		Values:      aws.StringValueSlice(partition.Values),
	}
}

func MockToPartition(mock *mock.MockObject) *glue.Partition {
	return &glue.Partition{
		DatabaseName: aws.String(mock.DbName),
		TableName:    aws.String(mock.TableName),
		Values:       aws.StringSlice(mock.Values),
		StorageDescriptor: &glue.StorageDescriptor{
			Columns:   mocksToColumns(mock.Columns),
			Location:  aws.String(mock.Location),
			SerdeInfo: &glue.SerDeInfo{Name: aws.String(mock.SdTableName)},
		},
	}
}

func MockToPartitions(mockPartitions []*mock.MockObject) []*glue.Partition {
	var partitions []*glue.Partition
	for _, partition := range mockPartitions {
		partitions = append(partitions, MockToPartition(partition))
	}
	return partitions
}

func (h GlueMsMock) GetPartition(partitonInput *glue.GetPartitionInput) (*glue.GetPartitionOutput, error) {
	partition, err := h.MockStore.GetPartition(aws.StringValue(partitonInput.DatabaseName), aws.StringValue(partitonInput.TableName), aws.StringValueSlice(partitonInput.PartitionValues))
	if err != nil {
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
		return nil, err
	}
	return &glue.GetTableOutput{Table: MockToTable(mockTable)}, nil
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
				Name:                 aws.String(table),
				SerializationLibrary: nil,
				Parameters:           nil,
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
			return fmt.Errorf("wrong partition location, expected: %s got: %s", expectedLocation, gotLocation)
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
	numOfPrtitions := 20
	initialPartitions := getNPartitions(tableName, location, numOfPrtitions)

	hiveMockClient := NewGlueMsMock()
	client := NewGlueMSClient(hiveMockClient, "")

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

	err = client.CopyOrMerge(dbName, tableName, branch, toDBName, toTableName, toBranch)
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
		t.Errorf("wrong location expected:%s got:%s", expectedLocation, gotLocation)
	}

	expectedColumns := getCols()
	gotColumns := copiedTable.StorageDescriptor.Columns
	for i, expectedColumn := range expectedColumns {
		if aws.StringValue(expectedColumn.Name) != aws.StringValue(gotColumns[i].Name) {
			t.Errorf("wrong column expected:%s got:%s ", aws.StringValue(expectedColumn.Name), aws.StringValue(gotColumns[i].Name))
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
	//compare partitions
	if len(copiedPartitions) != numOfPrtitions {
		t.Fatalf("got wrong amount of partitions expected:%d, got:%d", numOfPrtitions, len(copiedPartitions))
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
			t.Errorf("wrong column expected:%s got:%s ", aws.StringValue(expectedColumn.Name), aws.StringValue(gotColumns[i].Name))
		}
		if !ColumnEqual(expectedColumn, gotColumns[i]) {
			t.Fatalf("wrong column data for column %s", aws.StringValue(expectedColumn.Name))
		}
	}
	//add columns to existing partition
	parition19 := copiedPartitions[19]
	addColumn := &glue.Column{
		Name:    aws.String("column_three"),
		Type:    aws.String("string"),
		Comment: aws.String("added column before merge"),
	}
	parition19.StorageDescriptor.Columns = append(parition19.StorageDescriptor.Columns, addColumn)
	// add partition
	partitionNum := len(copiedPartitions)
	newPartition := getPartitionN(toTableName, expectedLocation, partitionNum)
	// add column
	newPartition.StorageDescriptor.Columns = append(newPartition.StorageDescriptor.Columns, addColumn)

	err = client.addPartitions(toDBName, toTableName, []*glue.Partition{newPartition})
	if err != nil {
		t.Fatal(err)
	}

	err = client.alterPartitions(toDBName, toTableName, []*glue.Partition{parition19})
	if err != nil {
		t.Fatal(err)
	}
	// now merge back
	err = client.CopyOrMerge(toDBName, toTableName, toBranch, dbName, tableName, branch)
	if err != nil {
		t.Fatal(err)
	}

	merged, err := client.getTable(dbName, tableName)
	mergedPartitions, err := client.getAllPartitions(dbName, tableName)
	mergedSdLocation := aws.StringValue(merged.StorageDescriptor.Location)
	if mergedSdLocation != location {
		t.Errorf("wrong location expected:%s got:%s", location, mergedSdLocation)

	}

	if len(mergedPartitions) != numOfPrtitions+1 {
		t.Fatalf("got wrong amount of partitions expected:%d, got:%d", numOfPrtitions+1, len(mergedPartitions))
	}

	// check if partition 21 was added and has three columns
	var partition19 *glue.Partition
	var partition20 *glue.Partition
	for _, partition := range mergedPartitions {
		if aws.StringValue(partition.Values[0]) == "part=20" {
			partition20 = partition
		}
		if aws.StringValue(partition.Values[0]) == "part=19" {
			partition19 = partition
		}
	}
	if partition20 == nil {
		t.Fatalf("expected to have a partition with value: part=20 after merge")
	}
	for _, mergedColumns := range [][]*glue.Column{partition19.StorageDescriptor.Columns, partition20.StorageDescriptor.Columns} {
		if len(mergedColumns) != 3 {
			t.Fatal("expected added partition to have 3 columns")
		}
		mcName := aws.StringValue(mergedColumns[2].Name)
		if mcName != "column_three" {
			t.Fatalf("expected added column to be column_three got:%s", mcName)
		}
	}

}
