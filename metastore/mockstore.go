package metastore

import (
	"fmt"
)

type MockObject struct {
	DbName      string
	TableName   string
	SdTableName string
	Location    string
}

type AlteredTable struct {
	dbName    string
	tableName string
	table     *MockObject
}
type AlteredPartitions struct {
	dbName     string
	tableName  string
	partitions []*MockObject
}

type MockStore struct {
	CreatedTable      *MockObject
	AlteredTable      *AlteredTable
	Tables            map[string]*MockObject
	partitionMap      map[string][]*MockObject
	addedPartitions   []*MockObject
	alteredPartitions *AlteredPartitions
	removedPartitions []*MockObject
}

func NewMockStrore() MockStore {
	return MockStore{
		CreatedTable:      nil,
		AlteredTable:      nil,
		Tables:            make(map[string]*MockObject),
		partitionMap:      nil,
		addedPartitions:   nil,
		alteredPartitions: nil,
		removedPartitions: nil,
	}
}

func (m MockStore) CreateTable(dbName, tableName, sdTableName, location string) {
	m.Tables[getKey(dbName, tableName)] = &MockObject{
		DbName:      dbName,
		TableName:   tableName,
		SdTableName: sdTableName,
		Location:    location,
	}
}

func getKey(dbName, tableName string) string {
	return fmt.Sprintf("%s-%s", dbName, tableName)
}
func (m MockStore) GetTable(dbname string, tableName string) *MockObject {
	return m.Tables[getKey(dbname, tableName)]
}

func (m MockStore) AlterTable(dbname string, tableName string, tDbName, tTableName, tSdTableName, tLocation string) {
	m.AlteredTable.dbName = dbname
	m.AlteredTable.tableName = tableName
	m.AlteredTable.table = &MockObject{
		DbName:      tDbName,
		TableName:   tTableName,
		SdTableName: tSdTableName,
		Location:    tLocation,
	}
}

func (m MockStore) AddPartitions(newPartitions []*MockObject) {
	m.addedPartitions = append(m.addedPartitions, newPartitions...)
}

func (m MockStore) GetPartitions(dbName string, tableName string) []*MockObject {
	return m.partitionMap[getKey(dbName, tableName)]
}

func (m MockStore) GetPartitionNames(db_name string, tbl_name string, max_parts int16) ([]string, error) {
	panic("implement me")
}

func (m MockStore) AlterPartitions(dbName string, tableName string, newPartitions []*MockObject) {
	m.alteredPartitions.dbName = dbName
	m.alteredPartitions.tableName = tableName
	m.alteredPartitions.partitions = newPartitions
}
