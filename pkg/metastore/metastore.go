package metastore

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// sparkSQLWorkaroundSuffix is a suffix added as a hack in Spark SQL, locations with this suffix are not used and should not be changed, Please refer to https://issues.apache.org/jira/browse/SPARK-15269 for more details.
	sparkSQLWorkaroundSuffix = "-__PLACEHOLDER__"
	// sparkSQLTableProviderKey  specifies the table is a Spark SQL data source table
	sparkSQLTableProviderKey    = "spark.sql.sources.provider"
	sparkSQLProviderLocationKey = "path"
)

func (m *Table) Update(db, table, serde string, transformLocation func(location string) (string, error), isSparkSQLTable bool) error {
	log := logging.Default().WithFields(logging.Fields{
		"db":    db,
		"table": table,
		"serde": serde,
	})
	if m.Sd == nil {
		m.Sd = &StorageDescriptor{}
	}
	m.DBName = db
	m.TableName = table
	err := m.Sd.Update(db, table, serde, transformLocation, isSparkSQLTable)
	if err != nil {
		log.WithError(err).WithField("table", spew.Sdump(*m)).Error("Update table")
		return err
	}
	log.WithField("table", spew.Sdump(*m)).Debug("Update table")
	return nil
}

func (m *Table) isSparkSQLTable() (res bool) {
	_, res = m.Parameters[sparkSQLTableProviderKey]
	return
}

func (m *Partition) Update(db, table, serde string, transformLocation func(location string) (string, error), isSparkSQLTable bool) error {
	log := logging.Default().WithFields(logging.Fields{
		"db":    db,
		"table": table,
		"serde": serde,
	})
	if m.Sd == nil {
		m.Sd = &StorageDescriptor{}
	}
	if m.Sd.SerdeInfo == nil {
		m.Sd.SerdeInfo = &SerDeInfo{}
	}
	m.DBName = db
	m.TableName = table
	m.Sd.SerdeInfo.Name = serde
	err := m.Sd.Update(db, table, serde, transformLocation, isSparkSQLTable)
	if err != nil {
		log.WithError(err).WithField("table", spew.Sdump(*m)).Error("Update table")
		return err
	}
	log.WithField("table", spew.Sdump(*m)).Debug("Update table")
	return nil
}

func (m *StorageDescriptor) Update(db, table, serde string, transformLocation func(location string) (string, error), isSparkSQLTable bool) error {
	if m.SerdeInfo == nil {
		m.SerdeInfo = &SerDeInfo{}
	}
	m.SerdeInfo.Name = serde
	var err error
	if m.Location != "" && !(isSparkSQLTable && strings.HasSuffix(m.Location, sparkSQLWorkaroundSuffix)) {
		m.Location, err = transformLocation(m.Location)
	}
	if err != nil {
		return err
	}

	if isSparkSQLTable {
		// Table was created by Spark SQL, we should change the internal stored Spark SQL location
		if l, ok := m.SerdeInfo.Parameters[sparkSQLProviderLocationKey]; ok {
			m.SerdeInfo.Parameters[sparkSQLProviderLocationKey], err = transformLocation(l)
		}
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
