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

type MSClient struct {
	context   context.Context
	client    ThriftHiveMetastoreClient
	transport thrift.TTransport
}

func NewMSClient(addr string, secure bool) (*MSClient, error) {
	msClient := &MSClient{}
	client, err := msClient.Open(addr, secure)
	if err != nil {
		return nil, err
	}
	msClient.client = client
	return msClient, nil
}

func (c *MSClient) Open(addr string, secure bool) (ThriftHiveMetastoreClient, error) {
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

func (c *MSClient) Close() error {
	if c.transport != nil {
		return c.transport.Close()
	}
	return nil
}

func (c *MSClient) CopyOrMerge(fromDB, fromTable, toDB, toTable, toBranch, serde string, partition []string) error {
	if len(partition) > 0 {
		return c.CopyPartition(fromDB, fromTable, toDB, toTable, toBranch, serde, partition)
	}
	table, err := c.client.GetTable(c.context, toDB, toTable)
	if err != nil {
		if _, ok := err.(*hive_metastore.NoSuchObjectException); !ok {
			return err
		}
	}
	if table == nil {
		return c.Copy(fromDB, fromTable, toDB, toTable, toBranch, serde)
	}
	return c.Merge(fromDB, fromTable, toDB, toTable, toBranch, serde)
}

func (c *MSClient) Copy(fromDB, fromTable, toDB, toTable, toBranch, serde string) error {
	table, err := c.client.GetTable(c.context, fromDB, fromTable)
	if err != nil {
		return err
	}
	table.DbName = toDB
	table.TableName = toTable
	if table.Sd != nil {
		if table.Sd.SerdeInfo != nil {
			table.Sd.SerdeInfo.Name = serde
		}
		table.Sd.Location, err = metastore.ReplaceBranchName(table.Sd.Location, toBranch)
		if err != nil {
			return err
		}
	}
	partitions, err := c.client.GetPartitions(c.context, fromDB, fromTable, -1)
	if err != nil {
		return err
	}
	for _, partition := range partitions {
		partition.DbName = toDB
		partition.TableName = toTable
		if partition.Sd != nil {
			if partition.Sd.SerdeInfo != nil {
				partition.Sd.SerdeInfo.Name = serde
			}
			partition.Sd.Location, err = metastore.ReplaceBranchName(partition.Sd.Location, toBranch)
			if err != nil {
				return err
			}
		}
	}
	err = c.client.CreateTable(c.context, table)
	if err != nil {
		return err
	}
	_, err = c.client.AddPartitions(c.context, partitions)
	return err
}

func (c *MSClient) Merge(fromDB, fromTable, toDB, toTable, toBranch, serde string) error {
	table, err := c.client.GetTable(c.context, fromDB, fromTable)
	if err != nil {
		return err
	}
	table.DbName = toDB
	table.TableName = toTable
	if table.Sd != nil {
		if table.Sd.SerdeInfo != nil {
			table.Sd.SerdeInfo.Name = serde
		}
		table.Sd.Location, err = metastore.ReplaceBranchName(table.Sd.Location, toBranch)
	}
	if err != nil {
		return err
	}
	partitions, err := c.client.GetPartitions(c.context, fromDB, fromTable, -1)
	if err != nil {
		return err
	}
	toPartitions, err := c.client.GetPartitions(c.context, toDB, toTable, -1)
	if err != nil {
		return err
	}

	partitionIter := NewPartitionCollection(partitions)
	toPartitionIter := NewPartitionCollection(toPartitions)
	var addPartitions, removePartitions, alterPartitions []*hive_metastore.Partition
	err = metastore.DiffIterable(partitionIter, toPartitionIter, func(difference catalog.DifferenceType, value interface{}, _ string) error {
		partition, ok := value.(*hive_metastore.Partition)
		if !ok {
			return fmt.Errorf("unexpected value in diffIterable call. expected to get  *hive_metastore.Partition, but got: %T", value)
		}

		partition.DbName = toDB
		partition.TableName = toTable
		if partition.Sd != nil {
			if partition.Sd.SerdeInfo != nil {
				partition.Sd.SerdeInfo.Name = toTable
			}
			partition.Sd.Location, err = metastore.ReplaceBranchName(partition.Sd.Location, toBranch)
			if err != nil {
				return err
			}
		}
		switch difference {
		case catalog.DifferenceTypeRemoved:
			removePartitions = append(removePartitions, partition)
		case catalog.DifferenceTypeAdded:
			addPartitions = append(addPartitions, partition)
		default:
			alterPartitions = append(alterPartitions, partition)
		}
		return nil
	})
	if err != nil {
		return err
	}

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

func (c *MSClient) CopyPartition(fromDB, fromTable, toDB, toTable, toBranch, serde string, partition []string) error {
	p1, err := c.client.GetPartition(c.context, fromDB, fromTable, partition)
	if err != nil {
		return err
	}
	p2, err := c.client.GetPartition(c.context, toDB, toTable, partition)
	if err != nil {
		if _, ok := err.(*hive_metastore.NoSuchObjectException); !ok {
			return err
		}
	}
	if p1.Sd != nil {
		if p1.Sd.SerdeInfo != nil {
			p1.Sd.SerdeInfo.Name = serde
		}
		p1.DbName = toDB
		p1.TableName = toTable
		p1.Sd.Location, err = metastore.ReplaceBranchName(p1.Sd.Location, toBranch)
	}
	if err != nil {
		return err
	}
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
	partitionIter := NewPartitionCollection(partitions)
	toPartitionIter := NewPartitionCollection(toPartitions)
	return metastore.Diff(partitionIter, toPartitionIter)
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

	colsIter := NewFSCollection(tableFrom.GetSd().GetCols())
	colsToIter := NewFSCollection(tableTo.GetSd().GetCols())
	return metastore.Diff(colsIter, colsToIter)
}
