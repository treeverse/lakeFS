package metastore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/metastore"
	mserrors "github.com/treeverse/lakefs/pkg/metastore/errors"
	"github.com/treeverse/lakefs/pkg/metastore/mock"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func getCols() []*metastore.FieldSchema {
	return []*metastore.FieldSchema{
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

func getPartitionValues(n int) []string {
	return []string{fmt.Sprintf("part=%d", n)}
}

func getPartitionN(db, table, location string, n int) *metastore.Partition {
	partitionValue := getPartitionValues(n)
	return &metastore.Partition{
		Values:    partitionValue,
		DBName:    db,
		TableName: table,
		Sd: &metastore.StorageDescriptor{
			Cols:         getCols(),
			Location:     fmt.Sprintf("%s/%s", location, partitionValue),
			InputFormat:  "parquet.hive.DeprecatedParquetInputFormat",
			OutputFormat: "parquet.hive.DeprecatedParquetOutputFormat",
			SerdeInfo: &metastore.SerDeInfo{
				Name:             table,
				SerializationLib: "",
			},
		},
	}
}

func getNPartitions(db, table, location string, n int) map[string]*metastore.Partition {
	partitions := make(map[string]*metastore.Partition)

	for i := 0; i < n; i++ {
		partition := getPartitionN(db, table, location, i)
		key := mock.GetPartitionKey(db, table, partition.Values)
		partitions[key] = partition
	}
	return partitions
}

func getLocation(repoLocation, branch, tableDir string) string {
	return fmt.Sprintf("%s/%s/%s", repoLocation, branch, tableDir)
}

func TestMSClient_CopySchema(t *testing.T) {
	tests := []struct {
		TestName         string
		SourceName       string
		DestinationName  string
		LocationURI      string
		destBranch       string
		ExpectedLocation string
		ExpectedError    error
	}{
		{
			TestName:         "with location",
			SourceName:       "source",
			DestinationName:  "destination",
			destBranch:       "branch1",
			LocationURI:      "s3://example/main/path/to/schema/",
			ExpectedLocation: "s3://example/branch1/path/to/schema/",
		},
		{
			TestName:         "table exists",
			SourceName:       "source",
			DestinationName:  "source",
			LocationURI:      "s3://example/path/to/schema/",
			destBranch:       "branch1",
			ExpectedLocation: "s3://example/branch1/path/to/schema/",
			ExpectedError:    mserrors.ErrSchemaExists,
		},
		{
			TestName:         "no location",
			SourceName:       "source",
			DestinationName:  "destination",
			destBranch:       "branch1",
			LocationURI:      "",
			ExpectedLocation: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.TestName, func(t *testing.T) {
			initialDatabases := make(map[string]*metastore.Database)

			initialDatabases[tt.SourceName] = &metastore.Database{Name: tt.SourceName, LocationURI: tt.LocationURI}
			client := mock.NewMSClient(t, initialDatabases, nil, nil)

			ctx := context.Background()
			err := metastore.CopyDB(ctx, client, client, tt.SourceName, tt.DestinationName, tt.destBranch, "")
			if !errors.Is(err, tt.ExpectedError) {
				t.Fatalf("expected err:%s got:%v", tt.ExpectedError, err)
			}
			if tt.ExpectedError != nil {
				return
			}
			destDB, err := client.GetDatabase(ctx, tt.DestinationName)
			if err != nil {
				t.Fatal(err)
			}
			if destDB.LocationURI != tt.ExpectedLocation {
				t.Errorf("wrong location expected %s, got %v", tt.ExpectedLocation, destDB.LocationURI)
			}
		})
	}
}

func TestMSClient_CopyAndMergeBack(t *testing.T) {
	tableName := "table"
	dbName := "default"
	repoLocation := "s3a://example"
	branch := "master"
	tableDir := "table_by_partition"
	location := getLocation(repoLocation, branch, tableDir)

	initialTable := &metastore.Table{
		DBName:    dbName,
		TableName: tableName,
		Sd: &metastore.StorageDescriptor{
			Cols:         getCols(),
			Location:     location,
			InputFormat:  "",
			OutputFormat: "",
		},
	}
	numOfPartitions := 20
	initialPartitions := getNPartitions(dbName, tableName, location, numOfPartitions)
	table := make(map[string]*metastore.Table)
	table[mock.GetKey(dbName, tableName)] = initialTable
	clientFrom := mock.NewMSClient(t, nil, table, initialPartitions)
	clientTo := mock.NewMSClient(t, nil, nil, nil)
	ctx := context.Background()

	toTableName := "copy_table"
	toDBName := "default"
	toBranch := "br1"

	err := metastore.CopyOrMerge(ctx, clientFrom, clientTo, dbName, tableName, toDBName, toTableName, toBranch, toTableName, nil, false, "")
	if err != nil {
		t.Fatal(err)
	}
	expectedTable := &metastore.Table{
		DBName:    toDBName,
		TableName: toTableName,
		Sd: &metastore.StorageDescriptor{
			Cols:     getCols(),
			Location: fmt.Sprintf("%s/%s/%s", repoLocation, toBranch, tableDir),
			SerdeInfo: &metastore.SerDeInfo{
				Name: toTableName,
			},
		},
	}
	copiedTable, err := clientTo.GetTable(ctx, toDBName, toTableName)
	if err != nil {
		t.Fatal(err)
	}

	expectedLocation := getLocation(repoLocation, toBranch, tableDir)

	if diff := deep.Equal(copiedTable, expectedTable); diff != nil {
		t.Error("ValidateTable() found diff", diff)
	}
	copiedPartitions, err := clientTo.GetPartitions(ctx, toDBName, toTableName)
	testutil.Must(t, err)
	// compare partitions

	expectedPartitions := getNPartitions(toDBName, toTableName, expectedLocation, numOfPartitions)

	expectedPartitionsMap := mock.PartitionsListToMap(copiedPartitions)
	if diff := deep.Equal(expectedPartitionsMap, expectedPartitions); diff != nil {
		t.Error("ValidatePartition() found diff", diff)
	}

	// verify first partition (enough)
	err = clientTo.DropPartition(ctx, toDBName, toTableName, []string{"part=17"})
	if err != nil {
		t.Fatal(err)
	}
	// add columns to existing partition
	partition19 := expectedPartitionsMap[mock.GetPartitionKey(toDBName, toTableName, []string{"part=19"})]
	addColumn := &metastore.FieldSchema{
		Name:    "column_three",
		Type:    "string",
		Comment: "added column before merge",
	}

	testutil.Must(t, err)
	mock.AddColumn(partition19, addColumn) // TODO(Guys): move this to mock
	// add partition
	partition20 := getPartitionN(toDBName, toTableName, expectedLocation, 20)
	// add column
	mock.AddColumn(partition20, addColumn)
	err = clientTo.AddPartitions(ctx, toTableName, toDBName, []*metastore.Partition{partition20})
	if err != nil {
		t.Fatal(err)
	}

	err = clientTo.AlterPartitions(ctx, toDBName, toTableName, []*metastore.Partition{partition19})
	if err != nil {
		t.Fatal(err)
	}
	// now merge back
	err = metastore.CopyOrMerge(ctx, clientTo, clientFrom, toDBName, toTableName, dbName, tableName, branch, toTableName, nil, false, "")
	if err != nil {
		t.Fatal(err)
	}
	merged, err := clientFrom.GetTable(ctx, dbName, tableName)
	testutil.Must(t, err)
	mergedPartitions, err := clientFrom.GetPartitions(ctx, dbName, tableName)
	testutil.Must(t, err)
	if merged.Sd.Location != location {
		t.Errorf("wrong location expected:%s got:%s", location, merged.Sd.Location)
	}
	if len(mergedPartitions) != numOfPartitions {
		t.Fatalf("got wrong amount of partitions expected:%d, got:%d", numOfPartitions, len(mergedPartitions))
	}
	// check if partition 20 was added and has three columns, 19 was updated and 17 was deleted
	m := make(map[string]*metastore.Partition)
	for _, partition := range mergedPartitions {
		m[partition.Values[0]] = partition
	}
	existingPartitions := []string{"part=19", "part=20"}
	for _, p := range existingPartitions {
		// check partition exists
		partition, ok := m[p]
		if !ok {
			t.Fatalf("expected to have a partition with value: %s after merge", p)
		}
		// check that columns are added
		columns := partition.Sd.Cols
		if len(columns) != 3 {
			t.Fatalf("expected added partition (%s) to have 3 columns, found %d", p, len(columns))
		}
		columnName := columns[2].Name
		if columnName != "column_three" {
			t.Fatalf("expected added column to be column_three got: %s", columnName)
		}
	}
	if _, ok := m["part=17"]; ok {
		t.Fatal("expected part=17 partition to be deleted")
	}
}
