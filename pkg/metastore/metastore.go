package metastore

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/glue"
)

func (m *Table) Update(db, table, serde string, transformLocation func(location string) (string, error)) error {
	if m.Sd == nil {
		m.Sd = &StorageDescriptor{}
	}
	if m.Sd.SerdeInfo == nil {
		m.Sd.SerdeInfo = &SerDeInfo{}
	}
	m.DBName = db
	m.TableName = table
	m.Sd.SerdeInfo.Name = serde
	var err error
	if m.Sd.Location != "" {
		m.Sd.Location, err = transformLocation(m.Sd.Location)
	}
	return err
}

func (m *Partition) Update(db, table, serde string, transformLocation func(location string) (string, error)) error {
	if m.Sd == nil {
		m.Sd = &StorageDescriptor{}
	}
	if m.Sd.SerdeInfo == nil {
		m.Sd.SerdeInfo = &SerDeInfo{}
	}
	m.DBName = db
	m.TableName = table
	m.Sd.SerdeInfo.Name = serde
	var err error
	if m.Sd.Location != "" {
		m.Sd.Location, err = transformLocation(m.Sd.Location)
	}
	return err
}

type Database struct {
	Name              string
	Description       string
	LocationURI       string
	Parameters        map[string]string
	HivePrivileges    interface{}
	OwnerName         *string
	HiveOwnerType     interface{}
	AWSTargetDatabase *glue.DatabaseIdentifier
}

type Table struct {
	TableName                        string
	DBName                           string
	Owner                            string
	CreateTime                       int64
	LastAccessTime                   int64
	Retention                        int
	Sd                               *StorageDescriptor
	PartitionKeys                    []*FieldSchema
	Parameters                       map[string]string
	ViewOriginalText                 string
	ViewExpandedText                 string
	TableType                        string
	Temporary                        bool
	RewriteEnabled                   *bool
	AWSCreatedBy                     *string
	AWSDescription                   *string
	AWSIsRegisteredWithLakeFormation *bool
	AWSLastAnalyzedTime              *time.Time
	AWSTargetTable                   interface{}
	AWSUpdateTime                    *time.Time
	Privileges                       interface{}
}

type Partition struct {
	Values              []string
	DBName              string
	TableName           string
	CreateTime          int
	LastAccessTime      int
	Sd                  *StorageDescriptor
	Parameters          map[string]string
	AWSLastAnalyzedTime *time.Time
	Privileges          interface{}
}

type StorageDescriptor struct {
	Cols                   []*FieldSchema
	Location               string
	InputFormat            string
	OutputFormat           string
	Compressed             bool
	NumBuckets             int
	SerdeInfo              *SerDeInfo
	BucketCols             []string
	SortCols               []*Order
	Parameters             map[string]string
	SkewedInfo             *SkewedInfo
	StoredAsSubDirectories *bool
	AWSSchemaReference     interface{}
}

type SerDeInfo struct {
	Name             string
	SerializationLib string
	Parameters       map[string]string
}

type FieldSchema struct {
	Name    string
	Type    string
	Comment string
}

type Order struct {
	Col   string
	Order int
}

type SkewedInfo struct {
	SkewedColNames             []string
	SkewedColValues            [][]string
	AWSSkewedColValues         []string //
	SkewedColValueLocationMaps map[string]string
}

func (m Partition) Name() string {
	return strings.Join(m.Values, "-")
}
