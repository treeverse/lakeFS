package hive

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/treeverse/lakefs/pkg/metastore"
	mserrors "github.com/treeverse/lakefs/pkg/metastore/errors"
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
	table, err := h.GetTable(ctx, dbname, tableName)
	var noSuchObjectErr *hive_metastore.NoSuchObjectException
	if err != nil && !errors.As(err, &noSuchObjectErr) {
		return false, err
	}
	return table != nil, nil
}

func (h *MSClient) GetTable(ctx context.Context, dbname string, tableName string) (*metastore.Table, error) {
	tb, err := h.Client.GetTable(ctx, dbname, tableName)
	if err != nil {
		return nil, err
	}
	return TableHiveToLocal(tb), nil
}

func (h *MSClient) AlterTable(ctx context.Context, dbName string, tableName string, newTable *metastore.Table) error {
	newHiveTable := TableLocalToHive(newTable)

	return h.Client.AlterTable(ctx, dbName, tableName, newHiveTable)
}

func (h *MSClient) AddPartitions(ctx context.Context, _ string, _ string, newParts []*metastore.Partition) error {
	newHivePartitions := PartitionsLocalToHive(newParts)
	_, err := h.Client.AddPartitions(ctx, newHivePartitions)
	return err
}

func (h *MSClient) GetPartitions(ctx context.Context, dbName string, tableName string) ([]*metastore.Partition, error) {
	partitions, err := h.Client.GetPartitions(ctx, dbName, tableName, -1)
	if err != nil {
		return nil, err
	}
	return PartitionsHiveToLocal(partitions), nil
}

func (h *MSClient) GetPartition(ctx context.Context, dbName string, tableName string, values []string) (*metastore.Partition, error) {
	partition, err := h.Client.GetPartition(ctx, dbName, tableName, values)
	if err != nil {
		return nil, err
	}
	return PartitionHiveToLocal(partition), nil
}

func (h *MSClient) AlterPartitions(ctx context.Context, dbName string, tableName string, newPartitions []*metastore.Partition) error {
	partitions := PartitionsLocalToHive(newPartitions)
	return h.Client.AlterPartitions(ctx, dbName, tableName, partitions)
}

func (h *MSClient) AlterPartition(ctx context.Context, dbName string, tableName string, partition *metastore.Partition) error {
	hivePartition := PartitionLocalToHive(partition)
	return h.Client.AlterPartition(ctx, dbName, tableName, hivePartition)
}

func (h *MSClient) AddPartition(ctx context.Context, _ string, _ string, newPartition *metastore.Partition) error {
	hivePartition := PartitionLocalToHive(newPartition)
	_, err := h.Client.AddPartition(ctx, hivePartition)
	return err
}

func (h *MSClient) DropPartition(ctx context.Context, dbName string, tableName string, values []string) error {
	_, err := h.Client.DropPartition(ctx, dbName, tableName, values, false)
	return err
}

func (h *MSClient) GetDatabase(ctx context.Context, name string) (*metastore.Database, error) {
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

func (h *MSClient) GetAllDatabases(ctx context.Context) ([]*metastore.Database, error) {
	databaseNames, err := h.Client.GetAllDatabases(ctx)
	if err != nil {
		return nil, err
	}
	return h.getDatabasesFromNames(ctx, databaseNames)
}

func (h *MSClient) CreateDatabase(ctx context.Context, database *metastore.Database) error {
	hiveDatabase := DatabaseLocalToHive(database)
	err := h.Client.CreateDatabase(ctx, hiveDatabase)
	var ErrExists *hive_metastore.AlreadyExistsException
	if errors.As(err, &ErrExists) {
		return mserrors.ErrSchemaExists
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
