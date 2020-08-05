package mock

import (
	"errors"
	"fmt"
	"strings"
)

var ErrNotFound = errors.New("not found")

type Column struct {
	Name    string
	Type    string
	Comment string
}

// table and partition
type MetastoreObject struct {
	DBName      string
	TableName   string
	SdTableName string
	Location    string
	Values      []string
	Columns     []*Column
}

type MetaStore struct {
	CreatedTable *MetastoreObject
	Tables       map[string]*MetastoreObject
	partitionMap map[string]*MetastoreObject
}

func NewMockStore() MetaStore {
	return MetaStore{
		Tables:       make(map[string]*MetastoreObject),
		partitionMap: make(map[string]*MetastoreObject),
	}
}

func getKey(dbName, tableName string) string {
	return fmt.Sprintf("%s-%s", dbName, tableName)
}

func (m MetaStore) CreateTable(dbName, tableName string, mockTable *MetastoreObject) error {
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

func (m MetaStore) GetTable(dbname string, tableName string) (*MetastoreObject, error) {
	key := getKey(dbname, tableName)
	table := m.Tables[key]
	if table == nil {
		return nil, fmt.Errorf("no table for key %s - %w", key, ErrNotFound)
	}
	return table, nil
}

func (m MetaStore) AlterTable(db, tableName string, table *MetastoreObject) error {
	key := getKey(db, tableName)
	if m.Tables[key] == nil {
		return fmt.Errorf("table with key does not exist %s", key)
	}
	m.Tables[getKey(db, tableName)] = table
	return nil
}

func (m MetaStore) AddPartition(newPartition *MetastoreObject) error {
	key := getPartitionKey(newPartition.DBName, newPartition.TableName, newPartition.Values)
	if m.partitionMap[key] != nil {
		return fmt.Errorf("partition for key %s already exists", key)
	}
	m.partitionMap[key] = newPartition
	return nil
}

func (m MetaStore) AddPartitions(newPartitions []*MetastoreObject) error {
	for _, partition := range newPartitions {
		err := m.AddPartition(partition)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m MetaStore) GetPartition(db string, table string, vals []string) (*MetastoreObject, error) {
	key := getPartitionKey(db, table, vals)
	partition := m.partitionMap[key]
	if partition == nil {
		return nil, fmt.Errorf("no partition for key %s - %w", key, ErrNotFound)
	}
	return partition, nil
}

func (m MetaStore) GetPartitions(dbName string, tableName string) []*MetastoreObject {
	k := getKey(dbName, tableName)
	var res []*MetastoreObject
	for key, object := range m.partitionMap {
		if object != nil && strings.HasPrefix(key, k) {
			res = append(res, object)
		}
	}
	return res
}
func (m MetaStore) AlterPartition(dbName string, tableName string, newPartition *MetastoreObject) error {
	key := getPartitionKey(dbName, tableName, newPartition.Values)
	if m.partitionMap[key] == nil {
		return fmt.Errorf("trying to alter missing partition with key %s", key)
	}
	m.partitionMap[key] = newPartition
	return nil
}

func (m MetaStore) AlterPartitions(dbName string, tableName string, newPartitions []*MetastoreObject) error {
	for _, partition := range newPartitions {
		err := m.AlterPartition(dbName, tableName, partition)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m MetaStore) DropPartition(db string, table string, partitions []string) error {
	key := getPartitionKey(db, table, partitions)
	if m.partitionMap[key] == nil {
		return fmt.Errorf("trying to remove missing partition with key %s", key)
	}
	delete(m.partitionMap, key)
	return nil
}
