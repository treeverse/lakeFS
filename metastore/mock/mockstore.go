package mock

import (
	"fmt"
	"strings"
)

type Column struct {
	Name    string
	Type    string
	Comment string
}

// table and partition
type MockObject struct {
	DbName      string
	TableName   string
	SdTableName string
	Location    string
	Values      []string
	Columns     []*Column
}

type MockStore struct {
	CreatedTable *MockObject
	Tables       map[string]*MockObject
	partitionMap map[string]*MockObject
}

func NewMockStore() MockStore {
	return MockStore{
		Tables:       make(map[string]*MockObject),
		partitionMap: make(map[string]*MockObject),
	}
}

func getKey(dbName, tableName string) string {
	return fmt.Sprintf("%s-%s", dbName, tableName)
}

func (m MockStore) CreateTable(dbName, tableName string, mockTable *MockObject) error {
	key := getKey(dbName, tableName)
	if m.Tables[key] != nil {
		return fmt.Errorf("table exists alredy with key %s", key)
	}
	m.Tables[getKey(dbName, tableName)] = mockTable
	return nil
}

func getPartitionKey(dbName, tableName string, partition []string) string {
	return fmt.Sprintf("%s-%s-%s", dbName, tableName, strings.Join(partition, "-"))
}

func (m MockStore) GetTable(dbname string, tableName string) (*MockObject, error) {
	key := getKey(dbname, tableName)
	table := m.Tables[key]
	if table == nil {
		return nil, fmt.Errorf("no table for key %s", key)
	}
	return table, nil
}

func (m MockStore) AlterTable(db, tableName string, table *MockObject) error {
	key := getKey(db, tableName)
	if m.Tables[key] == nil {
		return fmt.Errorf("table with key does not exist %s", key)
	}
	m.Tables[getKey(db, tableName)] = table
	return nil
}

func (m MockStore) AddPartition(newPartition *MockObject) error {
	key := getPartitionKey(newPartition.DbName, newPartition.TableName, newPartition.Values)
	if m.partitionMap[key] != nil {
		return fmt.Errorf("partition for key %s already exists", key)
	}
	m.partitionMap[key] = newPartition
	return nil
}

func (m MockStore) AddPartitions(newPartitions []*MockObject) error {
	for _, partition := range newPartitions {
		err := m.AddPartition(partition)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m MockStore) GetPartition(db string, table string, vals []string) (*MockObject, error) {
	key := getPartitionKey(db, table, vals)
	partition := m.partitionMap[key]
	if partition == nil {
		return nil, fmt.Errorf("no partition")
	}
	return partition, nil
}
func (m MockStore) GetPartitions(dbName string, tableName string) []*MockObject {
	var res []*MockObject
	for key, object := range m.partitionMap {
		if strings.HasPrefix(key, getKey(dbName, tableName)) {
			res = append(res, object)
		}
	}
	return res
}
func (m MockStore) AlterPartition(dbName string, tableName string, newPartition *MockObject) error {
	key := getPartitionKey(dbName, tableName, newPartition.Values)
	if m.partitionMap[key] == nil {
		return fmt.Errorf("trying to alter missing partition with key %s", key)
	}
	m.partitionMap[key] = newPartition
	return nil
}

func (m MockStore) AlterPartitions(dbName string, tableName string, newPartitions []*MockObject) error {
	for _, partition := range newPartitions {
		err := m.AlterPartition(dbName, tableName, partition)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m MockStore) DropPartition(db string, table string, vals []string) error {
	key := getPartitionKey(db, table, vals)
	if m.partitionMap[key] == nil {
		return fmt.Errorf("trying to remove missing partition with key %s", key)
	}
	m.partitionMap[key] = nil
	return nil
}
