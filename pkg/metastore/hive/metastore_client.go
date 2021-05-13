package hive

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/hive/gen-go/hive_metastore"
)

type ThriftHiveMetastoreClient interface {
	CreateTable(ctx context.Context, tbl *hive_metastore.Table) error
	GetTable(ctx context.Context, dbname string, tableName string) (*hive_metastore.Table, error)
	AlterTable(ctx context.Context, dbname string, tableName string, newTable *hive_metastore.Table) error
	AddPartitions(ctx context.Context, newParts []*hive_metastore.Partition) (int32, error)
	GetPartitions(ctx context.Context, dbName string, tableName string, maxPartitions int16) (r []*hive_metastore.Partition, err error)
	GetPartition(ctx context.Context, dbName string, tableName string, values []string) (r *hive_metastore.Partition, err error)
	AlterPartitions(ctx context.Context, dbName string, tableName string, newPartitions []*hive_metastore.Partition) error
	AlterPartition(ctx context.Context, dbName string, tableName string, values *hive_metastore.Partition) error
	AddPartition(ctx context.Context, newPartition *hive_metastore.Partition) (r *hive_metastore.Partition, err error)
	DropPartition(ctx context.Context, dbName string, tableName string, values []string, deleteData bool) (bool, error)
	GetDatabase(ctx context.Context, name string) (r *hive_metastore.Database, err error)
	GetDatabases(ctx context.Context, pattern string) (r []string, err error)
	GetAllDatabases(ctx context.Context) (r []string, err error)
	CreateDatabase(ctx context.Context, database *hive_metastore.Database) error
	GetTables(ctx context.Context, dbName string, pattern string) ([]string, error)
}

type MSClient struct {
	Client          ThriftHiveMetastoreClient
	transport       thrift.TTransport
	baseLocationURI string
}

func (h *MSClient) GetDBLocation(dbName string) string {
	return fmt.Sprintf("%s/%s.db", h.baseLocationURI, dbName)
}

func (h *MSClient) NormalizeDBName(name string) string {
	return strings.ReplaceAll(name, "-", "_") // Table names including `-` are allowed in Glue but not in Hive
}

func NewMSClient(addr string, secure bool, baseLocationURI string) (*MSClient, error) {
	msClient := &MSClient{
		baseLocationURI: strings.TrimRight(baseLocationURI, "/"),
	}
	err := msClient.open(addr, secure)
	if err != nil {
		return nil, err
	}
	return msClient, nil
}

func (h *MSClient) open(addr string, secure bool) error {
	var err error
	cfg := &thrift.TConfiguration{}
	if secure {
		cfg.TLSConfig = &tls.Config{
			//nolint:gosec
			InsecureSkipVerify: true,
		}
		h.transport, err = thrift.NewTSSLSocketConf(addr, cfg)
	} else {
		h.transport, err = thrift.NewTSocketConf(addr, cfg)
	}
	if err != nil {
		return err
	}
	err = h.transport.Open()
	if err != nil {
		return err
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryConf(cfg)
	iprot := protocolFactory.GetProtocol(h.transport)
	oprot := protocolFactory.GetProtocol(h.transport)
	h.Client = hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(iprot, oprot))
	return nil
}

func (h *MSClient) Close() error {
	if h.transport != nil {
		return h.transport.Close()
	}
	return nil
}

func (h *MSClient) CreateTable(ctx context.Context, tbl *metastore.Table) error {
	table := TableLocalToHive(tbl)
	err := h.Client.CreateTable(ctx, table)
	return err
}

func (h *MSClient) HasTable(ctx context.Context, dbname string, tableName string) (bool, error) {
	log := logging.Default().WithFields(logging.Fields{
		"db":    dbname,
		"table": tableName,
	})
	table, err := h.GetTable(ctx, dbname, tableName)
	var noSuchObjectErr *hive_metastore.NoSuchObjectException
	if err != nil && !errors.As(err, &noSuchObjectErr) {
		log.WithError(err).Error("HasTable")
		return false, err
	}
	result := table != nil
	log.WithField("result", result).Debug("HasTable")
	return result, nil
}

func (h *MSClient) GetTable(ctx context.Context, dbname string, tableName string) (*metastore.Table, error) {
	log := logging.Default().WithFields(logging.Fields{
		"db":    dbname,
		"table": tableName,
	})
	tb, err := h.Client.GetTable(ctx, dbname, tableName)
	if err != nil {
		log.WithError(err).Error("GetTable")
		return nil, err
	}

	tbl := TableHiveToLocal(tb)
	log.WithField("table", tbl).Debug("GetTable")
	return tbl, nil
}

func (h *MSClient) AlterTable(ctx context.Context, dbName string, tableName string, newTable *metastore.Table) error {
	newHiveTable := TableLocalToHive(newTable)
	log := logging.Default().WithFields(logging.Fields{
		"db":         dbName,
		"table_name": tableName,
		"table":      spew.Sdump(newTable),
		"hive_table": spew.Sdump(newHiveTable),
	})
	err := h.Client.AlterTable(ctx, dbName, tableName, newHiveTable)
	if err != nil {
		log.WithError(err).Error("AlterTable")
		return err
	}
	log.Debug("AlterTable")
	return nil
}

func (h *MSClient) AddPartitions(ctx context.Context, _ string, _ string, newParts []*metastore.Partition) error {
	newHivePartitions := PartitionsLocalToHive(newParts)
	_, err := h.Client.AddPartitions(ctx, newHivePartitions)
	return err
}

func (h *MSClient) GetPartitions(ctx context.Context, dbName string, tableName string) ([]*metastore.Partition, error) {
	log := logging.Default().WithFields(logging.Fields{
		"db":         dbName,
		"table_name": tableName,
	})
	partitions, err := h.Client.GetPartitions(ctx, dbName, tableName, -1)
	if err != nil {
		log.WithError(err).Error("GetPartitions")
		return nil, err
	}
	result := PartitionsHiveToLocal(partitions)
	log.WithField("partitions", spew.Sdump(result)).Debug("GetPartitions")
	return result, nil
}

func (h *MSClient) GetPartition(ctx context.Context, dbName string, tableName string, values []string) (*metastore.Partition, error) {
	log := logging.Default().WithFields(logging.Fields{
		"db":         dbName,
		"table_name": tableName,
	})
	partition, err := h.Client.GetPartition(ctx, dbName, tableName, values)
	if err != nil {
		log.WithError(err).Error("GetPartition")
		return nil, err
	}
	result := PartitionHiveToLocal(partition)
	log.WithField("partition", spew.Sdump(result)).Debug("GetPartition")
	return result, nil
}

func (h *MSClient) AlterPartitions(ctx context.Context, dbName string, tableName string, newPartitions []*metastore.Partition) error {
	log := logging.Default().WithFields(logging.Fields{
		"db":         dbName,
		"table_name": tableName,
	})
	partitions := PartitionsLocalToHive(newPartitions)
	err := h.Client.AlterPartitions(ctx, dbName, tableName, partitions)
	if err != nil {
		log.WithError(err).Error("AlterPartitions")
		return err
	}
	log.WithField("partitions", spew.Sdump(partitions)).Debug("AlterPartitions")
	return nil
}

func (h *MSClient) AlterPartition(ctx context.Context, dbName string, tableName string, partition *metastore.Partition) error {
	log := logging.Default().WithFields(logging.Fields{
		"db":         dbName,
		"table_name": tableName,
	})
	hivePartition := PartitionLocalToHive(partition)
	err := h.Client.AlterPartition(ctx, dbName, tableName, hivePartition)
	if err != nil {
		log.WithError(err).Error("AlterPartition")
		return err
	}
	log.WithField("partition", spew.Sdump(hivePartition)).Debug("AlterPartition")
	return nil
}

func (h *MSClient) AddPartition(ctx context.Context, dbName string, tableName string, newPartition *metastore.Partition) error {
	log := logging.Default().WithFields(logging.Fields{
		"db":         dbName,
		"table_name": tableName,
	})
	hivePartition := PartitionLocalToHive(newPartition)
	_, err := h.Client.AddPartition(ctx, hivePartition)
	if err != nil {
		log.WithError(err).Error("AddPartition")
		return err
	}
	log.WithField("partition", spew.Sdump(hivePartition)).Debug("AddPartition")
	return nil
}

func (h *MSClient) DropPartition(ctx context.Context, dbName string, tableName string, values []string) error {
	log := logging.Default().WithFields(logging.Fields{
		"db":         dbName,
		"table_name": tableName,
		"values":     values,
	})
	_, err := h.Client.DropPartition(ctx, dbName, tableName, values, false)
	if err != nil {
		log.WithError(err).Error("DropPartition")
		return err
	}
	log.Debug("DropPartition")
	return nil
}

func (h *MSClient) GetDatabase(ctx context.Context, name string) (*metastore.Database, error) {
	log := logging.Default().WithFields(logging.Fields{
		"name": name,
	})
	db, err := h.Client.GetDatabase(ctx, name)
	if err != nil {
		log.WithError(err).Error("GetDatabase")
		return nil, err
	}
	local := DatabaseHiveToLocal(db)
	log.WithField("database", local).Debug("GetDatabase")
	return local, nil
}

func (h *MSClient) GetDatabases(ctx context.Context, pattern string) ([]*metastore.Database, error) {
	log := logging.Default().WithFields(logging.Fields{
		"pattern": pattern,
	})
	databaseNames, err := h.Client.GetDatabases(ctx, pattern)
	if err != nil {
		log.WithError(err).Error("GetDatabases")
		return nil, err
	}
	result, err := h.getDatabasesFromNames(ctx, databaseNames)
	if err != nil {
		log.WithError(err).WithField("names", databaseNames).Error("getDatabasesFromNames")
		return nil, err
	}
	log.WithField("databases", spew.Sdump(result)).Debug("GetDatabases")
	return result, nil
}

func (h *MSClient) getDatabasesFromNames(ctx context.Context, names []string) ([]*metastore.Database, error) {
	databases := make([]*metastore.Database, len(names))
	for i, dbName := range names {
		hiveDatabase, err := h.Client.GetDatabase(ctx, dbName)
		if err != nil {
			return nil, err
		}
		database := DatabaseHiveToLocal(hiveDatabase)
		databases[i] = database
	}
	return databases, nil
}

func (h *MSClient) GetAllDatabases(ctx context.Context) ([]*metastore.Database, error) {
	databaseNames, err := h.Client.GetAllDatabases(ctx)
	if err != nil {
		return nil, err
	}
	return h.getDatabasesFromNames(ctx, databaseNames)
}

func (h *MSClient) CreateDatabaseIfNotExists(ctx context.Context, database *metastore.Database) error {
	hiveDatabase := DatabaseLocalToHive(database)
	err := h.Client.CreateDatabase(ctx, hiveDatabase)
	var errExists *hive_metastore.AlreadyExistsException
	if errors.As(err, &errExists) {
		return nil
	}
	return err
}

func (h *MSClient) GetTables(ctx context.Context, dbName string, pattern string) ([]*metastore.Table, error) {
	tableNames, err := h.Client.GetTables(ctx, dbName, pattern)
	if err != nil {
		return nil, err
	}
	return h.getTablesFromNames(ctx, dbName, tableNames)
}

func (h *MSClient) getTablesFromNames(ctx context.Context, dbName string, names []string) ([]*metastore.Table, error) {
	tables := make([]*metastore.Table, len(names))
	for i, tableName := range names {
		hiveTables, err := h.Client.GetTable(ctx, dbName, tableName)
		if err != nil {
			return nil, err
		}
		table := TableHiveToLocal(hiveTables)
		tables[i] = table
	}
	return tables, nil
}
