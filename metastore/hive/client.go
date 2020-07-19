package hive

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/metastore/hive/gen-go/hive_metastore"

	"github.com/treeverse/lakefs/metastore"
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
}

type Client struct {
	*MSClient
	Client    ThriftHiveMetastoreClient
	transport thrift.TTransport
}

func NewClient(addr string, secure bool) (*Client, error) {
	c := &Client{}
	if client, err := c.Open(addr, secure); err != nil {
		return nil, err
	} else {
		c.Client = client
	}
	c.MSClient = NewMSClient(c.Client)
	return c, nil
}

func (c *Client) Open(addr string, secure bool) (ThriftHiveMetastoreClient, error) {
	var err error
	if secure {
		cfg := new(tls.Config)
		cfg.InsecureSkipVerify = true
		c.transport, err = thrift.NewTSSLSocket(addr, cfg)
	} else {
		c.transport, err = thrift.NewTSocket(addr)
	}
	if err != nil {
		return nil, err
	}
	err = c.transport.Open()
	if err != nil {
		return nil, err
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	iprot := protocolFactory.GetProtocol(c.transport)
	oprot := protocolFactory.GetProtocol(c.transport)
	return hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(iprot, oprot)), nil
}

func (c *Client) Close() error {
	if c.transport != nil {
		return c.transport.Close()
	}
	return nil
}

func (c *Client) GetClient() ThriftHiveMetastoreClient {
	return c.client
}

type MSClient struct {
	context context.Context
	client  ThriftHiveMetastoreClient
}

func NewMSClient(client ThriftHiveMetastoreClient) *MSClient {
	return &MSClient{
		client: client,
	}
}

func (c *MSClient) CopyOrMerge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde string, partition []string) error {
	if partition != nil && len(partition) > 0 {
		return c.CopyPartition(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde, partition)
	}
	// todo: handle error (in case not found continue, is not found should be a function of the client that the mock will also implement)
	table, _ := c.client.GetTable(c.context, toDB, toTable)
	//if err != nil && !errors.Is(err, hive_metastore.NewNoSuchObjectException().) {
	//	fmt.Printf("%T", err)
	//	return err
	//}
	if table == nil {
		return c.Copy(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde)
	} else {
		return c.Merge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde)
	}
}

func (c *MSClient) Copy(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde string) error {
	table, err := c.client.GetTable(c.context, fromDB, fromTable)
	if err != nil {
		return err
	}
	table.DbName = toDB
	table.TableName = toTable
	table.Sd.SerdeInfo.Name = serde
	table.Sd.Location = metastore.TransformLocation(table.Sd.Location, fromBranch, toBranch)

	partitions, err := c.client.GetPartitions(c.context, fromDB, fromTable, 2000)
	for _, partition := range partitions {
		partition.Sd.Location = metastore.TransformLocation(partition.Sd.Location, fromBranch, toBranch)
		partition.TableName = toTable
		partition.Sd.SerdeInfo.Name = serde
		partition.DbName = toDB

	}
	err = c.client.CreateTable(c.context, table)
	if err != nil {
		return err
	}
	_, err = c.client.AddPartitions(c.context, partitions)
	return err
}

func (c *MSClient) Merge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde string) error {
	table, err := c.client.GetTable(c.context, fromDB, fromTable)
	if err != nil {
		return err
	}
	table.DbName = toDB
	table.TableName = toTable
	table.Sd.SerdeInfo.Name = serde
	table.Sd.Location = metastore.TransformLocation(table.Sd.Location, fromBranch, toBranch)

	partitions, err := c.client.GetPartitions(c.context, fromDB, fromTable, -1)
	if err != nil {
		return err
	}
	toPartitions, err := c.client.GetPartitions(c.context, toDB, toTable, -1)
	if err != nil {
		return err
	}

	partitionIter := NewPartitionIter(partitions)
	toPartitionIter := NewPartitionIter(toPartitions)
	var addPartitions, removePartitions, alterPartitions []*hive_metastore.Partition
	metastore.DiffIterable(partitionIter, toPartitionIter, func(difference catalog.DifferenceType, value interface{}, _ string) {
		partition, ok := value.(*hive_metastore.Partition)
		if !ok {
			msg := fmt.Sprintf("unexpected value in diffIterable call. expected to get  *hive_metastore.Partition, but got: %T", value)
			panic(msg)
		}

		partition.DbName = toDB
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

	err = c.client.AlterTable(c.context, toDB, toTable, table)
	if err != nil {
		return err
	}

	_, err = c.client.AddPartitions(c.context, addPartitions)
	if err != nil {
		return err
	}
	err = c.client.AlterPartitions(c.context, toDB, toTable, alterPartitions)
	if err != nil {
		return err
	}
	//drop one by one
	for _, partition := range removePartitions {
		_, err = c.client.DropPartition(c.context, toDB, toTable, partition.Values, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *MSClient) CopyPartition(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde string, partition []string) error {
	p1, err := c.client.GetPartition(c.context, fromDB, fromTable, partition)
	if err != nil {
		return err
	}
	p2, _ := c.client.GetPartition(c.context, toDB, toTable, partition)
	p1.DbName = toDB
	p1.TableName = toTable
	p1.Sd.SerdeInfo.Name = serde
	p1.Sd.Location = metastore.TransformLocation(p1.Sd.Location, fromBranch, toBranch)
	if p2 == nil {
		_, err = c.client.AddPartition(c.context, p1)
	} else {
		err = c.client.AlterPartition(c.context, toDB, toTable, p1)
	}
	return err
}

func (c *MSClient) Diff(fromDB, fromTable, toDB, toTable string) (*metastore.MetaDiff, error) {

	diffColumns, err := c.getColumnDiff(fromDB, fromTable, toDB, toTable)
	if err != nil {
		return nil, err
	}
	partitionDiff, err := c.getPartitionsDiff(fromDB, fromTable, toDB, toTable)
	if err != nil {
		return nil, err
	}
	return &metastore.MetaDiff{
		PartitionDiff: partitionDiff,
		ColumnsDiff:   diffColumns,
	}, nil
}

func (c *MSClient) getPartitionsDiff(fromDB string, fromTable string, toDB string, toTable string) (catalog.Differences, error) {
	partitions, err := c.client.GetPartitions(c.context, fromDB, fromTable, -1)
	if err != nil {
		return nil, err
	}
	toPartitions, err := c.client.GetPartitions(c.context, toDB, toTable, -1)
	if err != nil {
		return nil, err
	}
	partitionIter := NewPartitionIter(partitions)
	toPartitionIter := NewPartitionIter(toPartitions)
	return metastore.Diff(partitionIter, toPartitionIter), nil
}

func (c *MSClient) getColumnDiff(fromDB, fromTable, toDB, toTable string) (catalog.Differences, error) {

	tableFrom, err := c.client.GetTable(c.context, fromDB, fromTable)
	if err != nil {
		return nil, err
	}
	tableTo, err := c.client.GetTable(c.context, toDB, toTable)
	if err != nil {
		return nil, err
	}

	colsIter := NewFSIter(tableFrom.GetSd().GetCols())
	colsToIter := NewFSIter(tableTo.GetSd().GetCols())
	return metastore.Diff(colsIter, colsToIter), nil
}
