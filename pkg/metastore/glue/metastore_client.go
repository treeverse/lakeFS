package glue

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
	"github.com/treeverse/lakefs/pkg/metastore"
	mserrors "github.com/treeverse/lakefs/pkg/metastore/errors"
)

const MaxParts = 1000 // max possible 1000

type MSClient struct {
	client          glueiface.GlueAPI
	catalogID       string
	baseLocationURI string
}

func (g *MSClient) GetDBLocation(dbName string) string {
	return fmt.Sprintf("%s/%s", g.baseLocationURI, dbName)
}

func (g *MSClient) NormalizeDBName(db string) string {
	return db
}

func NewMSClient(cfg *aws.Config, catalogID, baselLocationURI string) (metastore.Client, error) {
	sess := session.Must(session.NewSession(cfg))
	sess.ClientConfig("glue")
	gl := glue.New(sess)
	return &MSClient{
		client:          gl,
		catalogID:       catalogID,
		baseLocationURI: strings.TrimRight(baselLocationURI, "/"),
	}, nil
}

func (g *MSClient) HasTable(ctx context.Context, dbName string, tableName string) (bool, error) {
	table, err := g.GetTable(ctx, dbName, tableName)
	var noSuchObjectErr *glue.EntityNotFoundException // TODO(Guys): validate this one
	if err != nil && !errors.As(err, &noSuchObjectErr) {
		return false, err
	}
	return table != nil, nil
}

func (g *MSClient) GetDatabase(ctx context.Context, name string) (*metastore.Database, error) {
	db, err := g.client.GetDatabaseWithContext(ctx, &glue.GetDatabaseInput{
		CatalogId: aws.String(g.catalogID),
		Name:      aws.String(name),
	})
	if err != nil {
		return nil, err
	}
	return DatabaseGlueToLocal(db.Database), nil
}

func (g *MSClient) getDatabaseFromGlue(ctx context.Context, token *string, parts int) (*glue.GetDatabasesOutput, error) {
	return g.client.GetDatabasesWithContext(ctx, &glue.GetDatabasesInput{
		CatalogId:         aws.String(g.catalogID),
		MaxResults:        aws.Int64(int64(parts)),
		NextToken:         token,
		ResourceShareType: nil,
	})
}

func (g *MSClient) GetDatabases(ctx context.Context, pattern string) ([]*metastore.Database, error) {
	var nextToken *string
	var allDatabases []*metastore.Database

	for {
		getDatabasesOutput, err := g.getDatabaseFromGlue(ctx, nextToken, MaxParts)
		if err != nil {
			return nil, err
		}
		nextToken = getDatabasesOutput.NextToken
		filteredDatabases, err := filterDatabases(getDatabasesOutput.DatabaseList, pattern)
		if err != nil {
			return nil, err
		}
		databases := DatabasesGlueToLocal(filteredDatabases)
		allDatabases = append(allDatabases, databases...)
		if nextToken == nil {
			break
		}
	}
	return allDatabases, nil
}

func filterDatabases(databases []*glue.Database, pattern string) ([]*glue.Database, error) {
	if pattern == "" {
		return databases, nil
	}
	r, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	res := make([]*glue.Database, 0)
	for _, database := range databases {
		if r.MatchString(aws.StringValue(database.Name)) {
			res = append(res, database)
		}
	}
	return res, nil
}

func (g *MSClient) GetTables(ctx context.Context, dbName string, pattern string) ([]*metastore.Table, error) {
	var nextToken *string
	allTables := make([]*metastore.Table, 0)
	for {
		getTablesOutput, err := g.client.GetTablesWithContext(ctx, &glue.GetTablesInput{
			CatalogId:    aws.String(g.catalogID),
			DatabaseName: aws.String(dbName),
			Expression:   aws.String(pattern),
			MaxResults:   aws.Int64(MaxParts),
			NextToken:    nextToken,
		})
		if err != nil {
			return nil, err
		}
		nextToken = getTablesOutput.NextToken

		tables := TablesGlueToLocal(getTablesOutput.TableList)
		allTables = append(allTables, tables...)
		if nextToken == nil {
			break
		}
	}
	return allTables, nil
}

func (g *MSClient) AlterTable(ctx context.Context, dbName string, _ string, newTable *metastore.Table) error {
	table := TableLocalToGlue(newTable)
	_, err := g.client.UpdateTableWithContext(ctx, &glue.UpdateTableInput{
		CatalogId:    aws.String(g.catalogID),
		DatabaseName: aws.String(dbName),
		SkipArchive:  aws.Bool(false), // UpdateTable always creates an archived version of the table before updating it. However, if skipArchive is set to true, UpdateTable does not create the archived version.
		TableInput:   table,
	})
	return err
}

func (g *MSClient) DropPartition(ctx context.Context, dbName string, tableName string, values []string) error {
	_, err := g.client.DeletePartitionWithContext(ctx, &glue.DeletePartitionInput{
		CatalogId:       aws.String(g.catalogID),
		DatabaseName:    aws.String(dbName),
		PartitionValues: aws.StringSlice(values),
		TableName:       aws.String(tableName),
	})
	return err
}

func (g *MSClient) CreateDatabase(ctx context.Context, database *metastore.Database) error {
	databaseInput := DatabaseLocalToGlue(database)
	_, err := g.client.CreateDatabaseWithContext(ctx, &glue.CreateDatabaseInput{
		CatalogId:     aws.String(g.catalogID),
		DatabaseInput: databaseInput,
	})
	var ErrExists *glue.AlreadyExistsException
	if errors.As(err, &ErrExists) {
		return mserrors.ErrSchemaExists
	}
	return err
}

func (g *MSClient) getTableData(ctx context.Context, dbName string, tblName string) (*glue.TableData, error) {
	table, err := g.client.GetTableWithContext(ctx,
		&glue.GetTableInput{
			CatalogId:    aws.String(g.catalogID),
			DatabaseName: aws.String(dbName),
			Name:         aws.String(tblName),
		})
	if err != nil {
		return nil, err
	}
	return table.Table, nil
}

func (g *MSClient) GetTable(ctx context.Context, dbName string, tableName string) (*metastore.Table, error) {
	table, err := g.getTableData(ctx, dbName, tableName)
	if err != nil {
		return nil, err
	}
	return TableGlueToLocal(table), nil
}

func (g *MSClient) CreateTable(ctx context.Context, tbl *metastore.Table) error {
	table := TableLocalToGlue(tbl)
	dbName := tbl.DBName
	_, err := g.client.CreateTableWithContext(ctx,
		&glue.CreateTableInput{
			CatalogId:    aws.String(g.catalogID),
			DatabaseName: aws.String(dbName),
			TableInput:   table,
		})
	return err
}

func (g *MSClient) GetPartition(ctx context.Context, dbName string, tableName string, values []string) (*metastore.Partition, error) {
	output, err := g.client.GetPartitionWithContext(ctx,
		&glue.GetPartitionInput{
			CatalogId:       aws.String(g.catalogID),
			DatabaseName:    aws.String(dbName),
			PartitionValues: aws.StringSlice(values),
			TableName:       aws.String(tableName),
		})
	if err != nil {
		return nil, err
	}
	return PartitionGlueToLocal(output.Partition), nil
}

func (g *MSClient) GetPartitions(ctx context.Context, dbName string, tableName string) ([]*metastore.Partition, error) {
	partitions, err := g.GetAllPartitions(ctx, dbName, tableName)
	if err != nil {
		return nil, err
	}

	return PartitionsGlueToLocal(partitions), nil
}

func (g *MSClient) getPartitionsFromGlue(ctx context.Context, dbName, tableName string, nextToken *string, maxParts int16) (*glue.GetPartitionsOutput, error) {
	return g.client.GetPartitionsWithContext(ctx,
		&glue.GetPartitionsInput{
			CatalogId:    aws.String(g.catalogID),
			DatabaseName: aws.String(dbName),
			MaxResults:   aws.Int64(int64(maxParts)),
			NextToken:    nextToken,
			TableName:    aws.String(tableName),
		})
}

func (g *MSClient) GetAllPartitions(ctx context.Context, dbName, tableName string) ([]*glue.Partition, error) {
	var nextToken *string
	var allPartitions []*glue.Partition
	for {
		getPartitionsOutput, err := g.getPartitionsFromGlue(ctx, dbName, tableName, nextToken, MaxParts)
		if err != nil {
			return nil, err
		}
		nextToken = getPartitionsOutput.NextToken
		partitions := getPartitionsOutput.Partitions
		allPartitions = append(allPartitions, partitions...)
		if nextToken == nil {
			break
		}
	}
	return allPartitions, nil
}

func (g *MSClient) AddPartition(ctx context.Context, tableName string, dbName string, newPartition *metastore.Partition) error {
	gluePartition := PartitionLocalToGlue(newPartition)
	_, err := g.client.CreatePartitionWithContext(ctx,
		&glue.CreatePartitionInput{
			CatalogId:      aws.String(g.catalogID),
			DatabaseName:   aws.String(dbName),
			PartitionInput: gluePartition,
			TableName:      aws.String(tableName),
		})
	return err
}

func (g *MSClient) AddPartitions(ctx context.Context, tableName string, dbName string, newParts []*metastore.Partition) error {
	gluePartitions := PartitionsLocalToGlue(newParts)

	partitionList := make([]*glue.PartitionInput, 0, len(gluePartitions))
	for _, partition := range gluePartitions {
		partitionList = append(partitionList, &glue.PartitionInput{
			LastAccessTime:    partition.LastAccessTime,
			LastAnalyzedTime:  partition.LastAnalyzedTime,
			Parameters:        partition.Parameters,
			StorageDescriptor: partition.StorageDescriptor,
			Values:            partition.Values,
		})
	}
	_, err := g.client.BatchCreatePartitionWithContext(ctx,
		&glue.BatchCreatePartitionInput{
			CatalogId:          aws.String(g.catalogID),
			DatabaseName:       aws.String(dbName),
			PartitionInputList: partitionList,
			TableName:          aws.String(tableName),
		})

	return err
}

func (g *MSClient) AlterPartition(ctx context.Context, dbName string, tableName string, partition *metastore.Partition) error {
	// No batch alter partitions we will need to do it one by one
	gluePartition := PartitionLocalToGlue(partition)

	_, err := g.client.UpdatePartitionWithContext(ctx,
		&glue.UpdatePartitionInput{
			CatalogId:          aws.String(g.catalogID),
			DatabaseName:       aws.String(dbName),
			PartitionInput:     gluePartition,
			PartitionValueList: gluePartition.Values,
			TableName:          aws.String(tableName),
		})
	return err
}

func (g *MSClient) AlterPartitions(ctx context.Context, dbName string, tableName string, newPartitions []*metastore.Partition) error {
	// No batch alter partitions we will need to do it one by one
	for _, partition := range newPartitions {
		err := g.AlterPartition(ctx, dbName, tableName, partition)
		if err != nil {
			return err
		}
	}
	return nil
}
