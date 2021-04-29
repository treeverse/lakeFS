package hive

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/treeverse/lakefs/pkg/metastore/hive/gen-go/hive_metastore"

	"github.com/treeverse/lakefs/pkg/metastore"
)

type ThriftHiveMetastoreClient interface {
	CreateTable(ctx context.Context, tbl *hive_metastore.Table) (err error)
	GetTable(ctx context.Context, dbname string, tableName string) (r *hive_metastore.Table, err error)
	AlterTable(ctx context.Context, dbname string, tableName string, newTable *hive_metastore.Table) (err error)
	AddPartitions(ctx context.Context, newParts []*hive_metastore.Partition) (r int32, err error)
	GetPartitions(ctx context.Context, dbName string, tableName string, maxPartitions int16) (r []*hive_metastore.Partition, err error)
	GetPartition(ctx context.Context, dbName string, tableName string, values []string) (r *hive_metastore.Partition, err error)
	AlterPartitions(ctx context.Context, dbName string, tableName string, newPartitions []*hive_metastore.Partition) (err error)
	AlterPartition(ctx context.Context, dbName string, tableName string, values *hive_metastore.Partition) (err error)
	AddPartition(ctx context.Context, newPartition *hive_metastore.Partition) (r *hive_metastore.Partition, err error)
	DropPartition(ctx context.Context, dbName string, tableName string, values []string, deleteData bool) (r bool, err error)
	GetDatabase(ctx context.Context, name string) (r *hive_metastore.Database, err error)
	GetDatabases(ctx context.Context, pattern string) (r []string, err error)
	GetAllDatabases(ctx context.Context) (r []string, err error)
	CreateDatabase(ctx context.Context, database *hive_metastore.Database) (err error)
	GetTables(ctx context.Context, dbName string, pattern string) (r []string, err error)
}

type MSClient struct {
	Client    ThriftHiveMetastoreClient
	transport thrift.TTransport
}

func (h *MSClient) NormalizeDBName(name string) string {
	return strings.ReplaceAll(name, "-", "_") // Table names including `-` are allowed in Glue but not in Hive
}

func NewMSClient(addr string, secure bool) (*MSClient, error) {
	msClient := &MSClient{}
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

func (h *MSClient) CreateTable(ctx context.Context, tbl *metastore.Table) (err error) {
	table := TableLocalToHive(tbl)
	return h.Client.CreateTable(ctx, table)
}

func (h *MSClient) HasTable(ctx context.Context, dbname string, tableName string) (hasTable bool, err error) {
	table, err := h.GetTable(ctx, dbname, tableName)
	var noSuchObjectErr *hive_metastore.NoSuchObjectException
	if err != nil && !errors.As(err, &noSuchObjectErr) {
		return false, err
	}
	return table != nil, nil
}

func (h *MSClient) GetTable(ctx context.Context, dbname string, tableName string) (r *metastore.Table, err error) {
	tb, err := h.Client.GetTable(ctx, dbname, tableName)
	if err != nil {
		return nil, err
	}
	return TableHiveToLocal(tb), nil
}

func (h *MSClient) AlterTable(ctx context.Context, dbname string, tableName string, newTable *metastore.Table) (err error) {
	newHiveTable := TableLocalToHive(newTable)

	return h.Client.AlterTable(ctx, dbname, tableName, newHiveTable)
}

func (h *MSClient) AddPartitions(ctx context.Context, tableName string, dbName string, newParts []*metastore.Partition) (err error) {
	newHivePartitions := PartitionsLocalToHive(newParts)
	_, err = h.Client.AddPartitions(ctx, newHivePartitions)
	return err
}

func (h *MSClient) GetPartitions(ctx context.Context, dbName string, tableName string) (r []*metastore.Partition, err error) {
	partitions, err := h.Client.GetPartitions(ctx, dbName, tableName, -1)
	if err != nil {
		return nil, err
	}
	return PartitionsHiveToLocal(partitions), nil
}

func (h *MSClient) GetPartition(ctx context.Context, dbName string, tableName string, values []string) (r *metastore.Partition, err error) {
	partition, err := h.Client.GetPartition(ctx, dbName, tableName, values)
	if err != nil {
		return nil, err
	}
	return PartitionHiveToLocal(partition), nil
}

func (h *MSClient) AlterPartitions(ctx context.Context, dbName string, tableName string, newPartitions []*metastore.Partition) (err error) {
	partitions := PartitionsLocalToHive(newPartitions)
	return h.Client.AlterPartitions(ctx, dbName, tableName, partitions)
}

func (h *MSClient) AlterPartition(ctx context.Context, dbName string, tableName string, values *metastore.Partition) (err error) {
	partition := PartitionLocalToHive(values)
	return h.Client.AlterPartition(ctx, dbName, tableName, partition)
}

func (h *MSClient) AddPartition(ctx context.Context, tableName string, dbName string, newPartition *metastore.Partition) (err error) {
	hivePartition := PartitionLocalToHive(newPartition)
	_, err = h.Client.AddPartition(ctx, hivePartition)
	return err
}

func (h *MSClient) DropPartition(ctx context.Context, dbName string, tableName string, values []string) error {
	_, err := h.Client.DropPartition(ctx, dbName, tableName, values, false)
	return err
}

func (h *MSClient) GetDatabase(ctx context.Context, name string) (r *metastore.Database, err error) {
	db, err := h.Client.GetDatabase(ctx, name)
	if err != nil {
		return nil, err
	}
	return DatabaseHiveToLocal(db), nil
}

func (h *MSClient) GetDatabases(ctx context.Context, pattern string) ([]*metastore.Database, error) {
	databaseNames, err := h.Client.GetDatabases(ctx, pattern)
	if err != nil {
		return nil, err
	}
	return h.getDatabasesFromNames(ctx, databaseNames)
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

func (h *MSClient) GetAllDatabases(ctx context.Context) (databases []*metastore.Database, err error) {
	databaseNames, err := h.Client.GetAllDatabases(ctx)
	if err != nil {
		return nil, err
	}
	return h.getDatabasesFromNames(ctx, databaseNames)
}

func (h *MSClient) CreateDatabaseIfNotExists(ctx context.Context, database *metastore.Database) error {
	hiveDatabase := DatabaseLocalToHive(database)
	err := h.Client.CreateDatabase(ctx, hiveDatabase)
	var ErrExists *hive_metastore.AlreadyExistsException
	if errors.As(err, &ErrExists) {
		return nil
	}
	return err
}

func (h *MSClient) GetTables(ctx context.Context, dbName string, pattern string) (tables []*metastore.Table, err error) {
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
