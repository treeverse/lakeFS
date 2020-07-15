package hive

import (
	"context"
	"crypto/tls"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/metastore/hive/gen-go/hive_metastore"

	"github.com/treeverse/lakefs/metastore"
)

type ThriftHiveMetastoreClient interface {
	CreateTable(ctx context.Context, tbl *hive_metastore.Table) (err error)
	GetTable(ctx context.Context, dbname string, tbl_name string) (r *hive_metastore.Table, err error)
	AlterTable(ctx context.Context, dbname string, tbl_name string, new_tbl *hive_metastore.Table) (err error)
	AddPartitions(ctx context.Context, new_parts []*hive_metastore.Partition) (r int32, err error)
	GetPartitions(ctx context.Context, db_name string, tbl_name string, max_parts int16) (r []*hive_metastore.Partition, err error)
	GetPartition(ctx context.Context, db_name string, tbl_name string, part_vals []string) (r *hive_metastore.Partition, err error)
	AlterPartitions(ctx context.Context, db_name string, tbl_name string, new_parts []*hive_metastore.Partition) (err error)
	AlterPartition(ctx context.Context, db_name string, tbl_name string, new_part *hive_metastore.Partition) (err error)
	AddPartition(ctx context.Context, new_part *hive_metastore.Partition) (r *hive_metastore.Partition, err error)
	DropPartition(ctx context.Context, db_name string, tbl_name string, part_vals []string, deleteData bool) (r bool, err error)
}

type HiveClientWrapper struct {
	transport thrift.TTransport
	addr      string
	secure    bool
	client    ThriftHiveMetastoreClient
}

func NewHiveClientWrapper(addr string, secure bool) *HiveClientWrapper {
	return &HiveClientWrapper{
		transport: nil,
		addr:      addr,
		secure:    secure,
	}
}

func (msc *HiveClientWrapper) Open() error {
	transportFactory := thrift.NewTTransportFactory()
	var err error
	if msc.secure {
		cfg := new(tls.Config)
		cfg.InsecureSkipVerify = true
		msc.transport, err = thrift.NewTSSLSocket(msc.addr, cfg)
	} else {
		msc.transport, err = thrift.NewTSocket(msc.addr)
	}
	if err != nil {
		return err
	}
	msc.transport, err = transportFactory.GetTransport(msc.transport)
	if err != nil {
		return err
	}
	err = msc.transport.Open()
	if err != nil {
		return err
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	iprot := protocolFactory.GetProtocol(msc.transport)
	oprot := protocolFactory.GetProtocol(msc.transport)
	msc.client = hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(iprot, oprot))
	return nil
}

func (msc *HiveClientWrapper) Close() error {
	if msc.transport != nil {
		return msc.transport.Close()
	}
	return nil
}

func (msc *HiveClientWrapper) GetClient() ThriftHiveMetastoreClient {
	return msc.client
}

type MSClient struct {
	context context.Context
	client  ThriftHiveMetastoreClient
}

func NewMetastoreClient(ctx context.Context, client ThriftHiveMetastoreClient) *MSClient {
	return &MSClient{
		context: ctx,
		client:  client,
	}
}

func (msc *MSClient) CopyOrMerge(fromDB, fromTable, fromBranch, ToDB, toTable, toBranch, serde string) error {
	table, _ := msc.client.GetTable(msc.context, ToDB, toTable)

	if table == nil {
		return msc.Copy(fromDB, fromTable, fromBranch, ToDB, toTable, toBranch, serde)
	} else {
		return msc.Merge(fromDB, fromTable, fromBranch, ToDB, toTable, toBranch, serde)
	}
}

func (msc *MSClient) Copy(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde string) error {
	table, err := msc.client.GetTable(msc.context, fromDB, fromTable)
	if err != nil {
		return err
	}
	if serde == "" {
		serde = toTable
	}
	table.DbName = toDB
	table.TableName = toTable
	table.Sd.SerdeInfo.Name = serde
	table.Sd.Location = metastore.TransformLocation(table.Sd.Location, fromBranch, toBranch)

	partitions, err := msc.client.GetPartitions(msc.context, fromDB, fromTable, 2000)
	for _, partition := range partitions {
		partition.Sd.Location = metastore.TransformLocation(partition.Sd.Location, fromBranch, toBranch)
		partition.TableName = toTable
		partition.Sd.SerdeInfo.Name = toTable
		partition.DbName = toDB

	}
	err = msc.client.CreateTable(msc.context, table)
	if err != nil {
		return err
	}
	_, err = msc.client.AddPartitions(msc.context, partitions)
	if err != nil {
		return err
	}
	return nil
}

func (msc *MSClient) Merge(fromDB, fromTable, fromBranch, toDBName, toTable, toBranch, serde string) error {
	table, err := msc.client.GetTable(msc.context, fromDB, fromTable)
	if err != nil {
		return err
	}
	table.DbName = toDBName
	table.TableName = toTable
	if serde == "" {
		serde = toTable
	}
	table.Sd.SerdeInfo.Name = serde
	table.Sd.Location = metastore.TransformLocation(table.Sd.Location, fromBranch, toBranch)

	partitions, err := msc.client.GetPartitions(msc.context, fromDB, fromTable, -1)
	if err != nil {
		return err
	}
	toPartitions, err := msc.client.GetPartitions(msc.context, toDBName, toTable, -1)
	if err != nil {
		return err
	}

	partitionIter := NewPartitionIter(partitions)
	toPartitionIter := NewPartitionIter(toPartitions)
	var addPartitions, removePartitions, alterPartitions []*hive_metastore.Partition
	metastore.Diff(partitionIter, toPartitionIter, func(difference catalog.DifferenceType, iter metastore.ComparableIterator) {
		partition := iter.(*PartitionIter).getCurrent()
		partition.DbName = toDBName
		partition.TableName = toTable
		partition.Sd.Location = metastore.TransformLocation(partition.Sd.Location, fromBranch, toBranch)
		partition.Sd.SerdeInfo.Name = toTable
		switch difference {
		case catalog.DifferenceTypeRemoved:
			removePartitions = append(removePartitions, partition)
		case catalog.DifferenceTypeAdded:
			addPartitions = append(addPartitions, partition)
		default:
			alterPartitions = append(alterPartitions, partition)
		}
	})

	err = msc.client.AlterTable(msc.context, toDBName, toTable, table)
	if err != nil {
		return err
	}

	_, err = msc.client.AddPartitions(msc.context, addPartitions)
	if err != nil {
		return err
	}
	err = msc.client.AlterPartitions(msc.context, toDBName, toTable, alterPartitions)
	if err != nil {
		return err
	}
	//drop one by one - todo - consider using drop partitionReq
	for _, partition := range removePartitions {
		_, err = msc.client.DropPartition(msc.context, toDBName, toTable, partition.Values, true) //todo: check if deleteData
		if err != nil {
			return err
		}
	}
	return nil
}

func (msc *MSClient) CopyPartition(fromDB, fromTable, fromBranch, toDB, toTable, toBranch string, partition []string) error {
	p1, err := msc.client.GetPartition(msc.context, fromDB, fromTable, partition)
	if err != nil {
		return err
	}
	p2, _ := msc.client.GetPartition(msc.context, toDB, toTable, partition)

	p1.DbName = toDB
	p1.TableName = toTable
	p1.Sd.SerdeInfo.Name = toTable
	p1.Sd.Location = metastore.TransformLocation(p1.Sd.Location, fromBranch, toBranch)
	if p2 == nil {
		_, err = msc.client.AddPartition(msc.context, p1)
	} else {
		err = msc.client.AlterPartition(msc.context, toDB, toTable, p1)
	}
	return err
}

func (msc *MSClient) Diff(fromDB, fromTable, toDB, toTable string) (*metastore.MetaDiff, error) {

	diffColumns, err := msc.getColumnDiff(fromDB, fromTable, toDB, toTable)
	if err != nil {
		return nil, err
	}
	partitionDiff, err := msc.getPartitionsDiff(fromDB, fromTable, toDB, toTable)
	if err != nil {
		return nil, err
	}
	return &metastore.MetaDiff{
		PartitionDiff: partitionDiff,
		ColumnsDiff:   diffColumns,
	}, nil
}

func (msc *MSClient) getPartitionsDiff(fromDB string, fromTable string, toDB string, toTable string) (catalog.Differences, error) {
	partitions, err := msc.client.GetPartitions(msc.context, fromDB, fromTable, -1)
	if err != nil {
		return nil, err
	}
	toPartitions, err := msc.client.GetPartitions(msc.context, toDB, toTable, -1)
	if err != nil {
		return nil, err
	}
	partitionIter := NewPartitionIter(partitions)
	toPartitionIter := NewPartitionIter(toPartitions)
	return metastore.GetDiff(partitionIter, toPartitionIter), nil
}

func (msc *MSClient) getColumnDiff(fromDB, fromTable, toDB, toTable string) (catalog.Differences, error) {

	tableFrom, err := msc.client.GetTable(msc.context, fromDB, fromTable)
	if err != nil {
		return nil, err
	}
	tableTo, err := msc.client.GetTable(msc.context, toDB, toTable)
	if err != nil {
		return nil, err
	}

	colsIter := NewFSIter(tableFrom.GetSd().GetCols())
	colsToIter := NewFSIter(tableTo.GetSd().GetCols())
	return metastore.GetDiff(colsIter, colsToIter), nil
}
