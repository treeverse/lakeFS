package hive

import (
	"context"
	"crypto/tls"
	"errors"
	"strconv"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/metastore/hive/thrift/gen-go/hive_metastore"

	"github.com/treeverse/lakefs/metastore"
)

type MSClient struct {
	context   context.Context
	client    *hive_metastore.ThriftHiveMetastoreClient
	transport thrift.TTransport
	addr      string
	secure    bool
}

type Option func(index *MSClient)

func WithClient(client *hive_metastore.ThriftHiveMetastoreClient) Option {
	return func(hmsc *MSClient) {
		hmsc.client = client
	}
}
func WithTransport(transport thrift.TTransport) Option {
	return func(hmsc *MSClient) {
		hmsc.transport = transport
	}
}

func NewMetastoreClient(ctx context.Context, addr string, secure bool, opts ...Option) *MSClient {
	hmsClient := &MSClient{
		context: ctx,
		addr:    addr,
		secure:  secure,
	}
	for _, opt := range opts {
		opt(hmsClient)
	}
	return hmsClient
}

type ThriftHiveMetastoreClient interface {
	CreateTable(ctx context.Context, tbl *hive_metastore.Table) (err error)
	GetTable(ctx context.Context, dbname string, tbl_name string) (r *hive_metastore.Table, err error)
	AlterTable(ctx context.Context, dbname string, tbl_name string, new_tbl *hive_metastore.Table) (err error)
	AddPartitions(ctx context.Context, new_parts []*hive_metastore.Partition) (r int32, err error)
	GetPartitions(ctx context.Context, db_name string, tbl_name string, max_parts int16) (r []*hive_metastore.Partition, err error)
	GetPartitionNames(ctx context.Context, db_name string, tbl_name string, max_parts int16) (r []string, err error)
	AlterPartitions(ctx context.Context, db_name string, tbl_name string, new_parts []*hive_metastore.Partition) (err error)
}

func (msc *MSClient) Open() error {
	// todo - remove if == nil , if I decide not to support optio for tests
	if msc.transport == nil {
		transportFactory := thrift.NewTTransportFactory() // TODO: check if want buffered or/and framed
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
	}

	if msc.client == nil {
		protocolFactory := thrift.NewTBinaryProtocolFactoryDefault() // TODO: check if want binary and if default is ok
		iprot := protocolFactory.GetProtocol(msc.transport)
		oprot := protocolFactory.GetProtocol(msc.transport)
		msc.client = hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(iprot, oprot))
	}
	return nil
}

func (msc *MSClient) Close() error {
	return msc.transport.Close()
}

func (msc *MSClient) CopyOrMerge(fromDBName, fromTable, fromBranch, ToDBName, toTable, toBranch string) error {
	table, err := msc.client.GetTable(msc.context, ToDBName, toTable)
	if err != nil && errors.Is(&hive_metastore.NoSuchObjectException{}, err) { //TODO: continue if error is not exists
		return err
	}
	if table == nil {
		return msc.Copy(fromDBName, fromTable, fromBranch, ToDBName, toTable, toBranch)
	} else {
		return msc.Merge(fromDBName, fromTable, fromBranch, ToDBName, toTable, toBranch)
	}
}

func (msc *MSClient) Copy(fromDBName, fromTable, fromBranch, ToDBName, toTable, toBranch string) error {
	table, err := msc.client.GetTable(msc.context, fromDBName, fromTable)
	if err != nil {
		return err
	}
	table.DbName = ToDBName
	table.TableName = toTable
	table.Sd.SerdeInfo.Name = toTable //todo double check this, maybe serde should be somehting else
	table.Sd.Location = metastore.TransformLocation(table.Sd.Location, fromBranch, toBranch)

	partitions, err := msc.client.GetPartitions(msc.context, fromDBName, fromTable, -1)
	for _, partition := range partitions {
		partition.Sd.Location = metastore.TransformLocation(partition.Sd.Location, fromBranch, toBranch)
		partition.TableName = toTable
		partition.Sd.SerdeInfo.Name = toTable
		partition.DbName = ToDBName

		// todo : probably remove this - byt consider adding data on partitions
		const LakeFSCreatedParameter = "lakefs-created"
		const LakeFSLastAccessParameter = "lakefs-accessed"
		partition.Parameters[LakeFSLastAccessParameter] = strconv.FormatInt(int64(partition.GetLastAccessTime()), 10)
		partition.Parameters[LakeFSCreatedParameter] = strconv.FormatInt(int64(partition.GetCreateTime()), 10)
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

func (msc *MSClient) CopyPartition(fromDBName, fromTable, fromBranch, ToDBName, toTable, toBranch string, partition []string) error {
	p1, err := msc.client.GetPartition(msc.context, fromDBName, fromTable, partition)
	if err != nil {
		return err
	}
	p2, err := msc.client.GetPartition(msc.context, fromDBName, fromTable, partition)
	if err != nil { //TODO: continue if error is not exists
		return err
	}
	p1.DbName = ToDBName
	p1.TableName = toTable
	p1.Sd.SerdeInfo.Name = toTable
	p1.Sd.Location = metastore.TransformLocation(p1.Sd.Location, fromBranch, toBranch)
	if p2 == nil {
		_, err = msc.client.AddPartition(msc.context, p1)
	} else {
		err = msc.client.AlterPartition(msc.context, ToDBName, toTable, p1)
	}
	return err
}

func (msc *MSClient) Merge(fromDBName, fromTable, fromBranch, toDBName, toTable, toBranch string) error {
	table, err := msc.client.GetTable(msc.context, fromDBName, fromTable)
	if err != nil {
		return err
	}
	table.DbName = toDBName
	table.TableName = toTable
	table.Sd.SerdeInfo.Name = toTable //todo double check this, maybe serde should be somehting else
	table.Sd.Location = metastore.TransformLocation(table.Sd.Location, fromBranch, toBranch)

	partitions, err := msc.client.GetPartitions(msc.context, fromDBName, fromTable, -1)
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
	//addPartitions, removePartitions, alterPartitions := getPartitionChanges(partitions, toPartitions, toDBName, toTable, toBranch, fromBranch)

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

func (msc *MSClient) Diff(FromDBName, fromTable, toDBName, toTable string) (*metastore.MetaDiff, error) {

	diffColumns, err := msc.getColumnDiff(FromDBName, fromTable, toDBName, toTable)
	if err != nil {
		return nil, err
	}
	partitionDiff, err := msc.getPartitionsDiff(FromDBName, fromTable, toDBName, toTable)
	if err != nil {
		return nil, err
	}
	return &metastore.MetaDiff{
		PartitionDiff: partitionDiff,
		ColumnsDiff:   diffColumns,
	}, nil
}

func (msc *MSClient) getPartitionsDiff(FromDBName string, fromTable string, toDBName string, toTable string) (catalog.Differences, error) {
	partitions, err := msc.client.GetPartitions(msc.context, FromDBName, fromTable, -1)
	if err != nil {
		return nil, err
	}
	toPartitions, err := msc.client.GetPartitions(msc.context, toDBName, toTable, -1)
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
