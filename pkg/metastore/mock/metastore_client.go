package mock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/pkg/metastore"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

type MSClient struct {
	t            *testing.T
	Tables       map[string]*metastore.Table
	partitionMap map[string]*metastore.Partition
}

func (m *MSClient) NormalizeDBName(name string) string {
	return name
}

func NewMSClient(t *testing.T, initialTable map[string]*metastore.Table, initialPartitions map[string]*metastore.Partition) *MSClient {
	if initialPartitions == nil {
		initialPartitions = make(map[string]*metastore.Partition)
	}
	if initialTable == nil {
		initialTable = make(map[string]*metastore.Table)
	}
	return &MSClient{
		t:            t,
		Tables:       initialTable,
		partitionMap: initialPartitions,
	}
}

func (m *MSClient) GetTable(_ context.Context, dbname string, tableName string) (r *metastore.Table, err error) {
	m.t.Helper()
	table := m.Tables[GetKey(dbname, tableName)]
	if table == nil {
		return nil, ErrNotFound
	}
	return table, nil
}

func (m *MSClient) HasTable(_ context.Context, dbname string, tableName string) (hasTable bool, err error) {
	_, hasTable = m.Tables[GetKey(dbname, tableName)]
	return hasTable, err
}

func (m *MSClient) GetPartitions(_ context.Context, dbName string, tableName string) (r []*metastore.Partition, err error) {
	key := GetKey(dbName, tableName)
	return GetByPrefix(m.partitionMap, key), nil
}

func (m *MSClient) GetPartition(_ context.Context, dbName string, tableName string, values []string) (r *metastore.Partition, err error) {
	return m.partitionMap[GetPartitionKey(dbName, tableName, values)], nil
}

func (m *MSClient) GetDatabase(_ context.Context, _ string) (r *metastore.Database, err error) {
	panic("implement me")
}

func (m *MSClient) GetDatabases(_ context.Context, _ string) (databases []*metastore.Database, err error) {
	panic("implement me")
}

func (m *MSClient) GetTables(_ context.Context, _ string, _ string) (tables []*metastore.Table, err error) {
	panic("implement me")
}

func (m *MSClient) GetPartitionCollection(_ context.Context, _ string, _ string) (metastore.Collection, error) {
	panic("implement me")
}

func (m *MSClient) GetTableCollection(_ context.Context, _ string, _ string) (metastore.Collection, error) {
	panic("implement me")
}

func (m *MSClient) CreateTable(_ context.Context, table *metastore.Table) error {
	key := GetKey(table.DBName, table.TableName)
	if m.Tables[key] != nil {
		return fmt.Errorf("table %w - key %s", ErrAlreadyExists, key)
	}
	m.Tables[key] = table
	return nil
}

func (m *MSClient) AlterTable(_ context.Context, dbname string, tableName string, newTable *metastore.Table) error {
	key := GetKey(dbname, tableName)
	if m.Tables[key] == nil {
		return fmt.Errorf("table %w - key %s", ErrNotFound, key) // TODO(Guys): consider using m.t.Fatal()
	}
	m.Tables[key] = newTable
	return nil
}

func (m *MSClient) AddPartitions(ctx context.Context, tableName string, dbName string, newParts []*metastore.Partition) error {
	for _, partition := range newParts {
		err := m.AddPartition(ctx, tableName, dbName, partition)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MSClient) AlterPartitions(ctx context.Context, dbName string, tableName string, newPartitions []*metastore.Partition) error {
	for _, partition := range newPartitions {
		err := m.AlterPartition(ctx, dbName, tableName, partition)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MSClient) AlterPartition(_ context.Context, dbName string, tableName string, partition *metastore.Partition) error {
	key := GetPartitionKey(dbName, tableName, partition.Values)
	if m.partitionMap[key] == nil {
		return fmt.Errorf("partition %w - key %s", ErrNotFound, key)
	}
	m.partitionMap[key] = partition
	return nil
}

func (m *MSClient) AddPartition(_ context.Context, tableName string, dbName string, partition *metastore.Partition) error {
	key := GetPartitionKey(dbName, tableName, partition.Values)
	if m.partitionMap[key] != nil {
		return fmt.Errorf("partition %w - key %s", ErrAlreadyExists, key)
	}
	m.partitionMap[key] = partition
	return nil
}

func (m *MSClient) DropPartition(_ context.Context, dbName string, tableName string, values []string) error {
	key := GetPartitionKey(dbName, tableName, values)
	if m.partitionMap[key] == nil {
		return fmt.Errorf("partition %w - key %s", ErrNotFound, key)
	}
	delete(m.partitionMap, key)
	return nil
}

func (m *MSClient) CreateDatabaseIfNotExists(_ context.Context, _ *metastore.Database) error {
	panic("implement me")
}

func PartitionsListToMap(partitions []*metastore.Partition) map[string]*metastore.Partition {
	res := make(map[string]*metastore.Partition)
	for _, partition := range partitions {
		res[GetPartitionKey(partition.DBName, partition.TableName, partition.Values)] = partition
	}
	return res
}

func AddColumn(m *metastore.Partition, col *metastore.FieldSchema) {
	m.Sd.Cols = append(m.Sd.Cols, col)
}

func GetByPrefix(m map[string]*metastore.Partition, prefix string) []*metastore.Partition {
	res := make([]*metastore.Partition, 0)
	for key, val := range m {
		if strings.HasPrefix(key, prefix) {
			res = append(res, val)
		}
	}
	return res
}

func GetKey(dbName, tableName string) string {
	return fmt.Sprintf("%s-%s", dbName, tableName)
}

func GetPartitionKey(dbName, tableName string, partition []string) string {
	return fmt.Sprintf("%s-%s-%s", dbName, tableName, strings.Join(partition, "-"))
}
