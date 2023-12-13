package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

type DatabricksClient struct {
	workspaceClient *databricks.WorkspaceClient
	ctx             context.Context
}

func (dbc *DatabricksClient) createExternalTable(warehouseID, catalogName, schemaName, tableName, location string) (string, error) {
	statement := fmt.Sprintf(`CREATE EXTERNAL TABLE %s LOCATION '%s'`, tableName, location)
	esr, err := dbc.workspaceClient.StatementExecution.ExecuteAndWait(dbc.ctx, sql.ExecuteStatementRequest{
		WarehouseId: warehouseID,
		Catalog:     catalogName,
		Schema:      schemaName,
		Statement:   statement,
	})
	if err != nil {
		return "", err
	}
	return esr.Status.State.String(), nil
}

func (dbc *DatabricksClient) dropTable(catalogName, schemaName, tableName string) error {
	tableFullName := fmt.Sprintf("%s.%s.%s", catalogName, schemaName, tableName)
	return dbc.workspaceClient.Tables.DeleteByFullName(dbc.ctx, tableFullName)
}

func (dbc *DatabricksClient) getOrCreateSchema(catalogName, schemaName string) (string, error) {
	// Full name of schema, in form of <catalog_name>.<schema_name>
	schemaAns, err := dbc.workspaceClient.Schemas.GetByFullName(dbc.ctx, fmt.Sprintf("%s.%s", catalogName, schemaName))
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			schemaAns, err = dbc.workspaceClient.Schemas.Create(dbc.ctx, catalog.CreateSchema{
				Name:        schemaName,
				CatalogName: catalogName,
			})
			if err != nil {
				return "", err
			}
			return schemaAns.Name, nil
		} else {
			return "", err
		}
	}
	return schemaAns.Name, nil
}

func newDatabricksClient(l *lua.State) *databricks.WorkspaceClient {
	host := lua.CheckString(l, -2)
	token := lua.CheckString(l, -1)
	return databricks.Must(databricks.NewWorkspaceClient(
		&databricks.Config{
			Host:        host,
			Token:       token,
			Credentials: config.PatCredentials{},
		},
	))
}

func registerExternalTable(client *DatabricksClient) lua.Function {
	return func(l *lua.State) int {
		tableName := lua.CheckString(l, 1)
		location := lua.CheckString(l, 2)
		warehouseID := lua.CheckString(l, 3)
		catalogName := lua.CheckString(l, 4)
		schemaName := lua.CheckString(l, 5)

		status, err := client.createExternalTable(warehouseID, catalogName, schemaName, tableName, location)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				err = client.dropTable(catalogName, schemaName, tableName)
				if err != nil {
					lua.Errorf(l, err.Error())
					panic("failed dropping an existing table")
				}
				status, err = client.createExternalTable(warehouseID, catalogName, schemaName, tableName, location)
				if err != nil {
					lua.Errorf(l, err.Error())
					panic("failed creating table")
				}
			} else {
				lua.Errorf(l, err.Error())
				panic("failed creating table")
			}
		}
		l.PushString(status)
		return 1
	}
}

func createSchema(client *DatabricksClient) lua.Function {
	return func(l *lua.State) int {
		ref := lua.CheckString(l, 1)
		catalogName := lua.CheckString(l, 2)
		schemaName, err := client.getOrCreateSchema(catalogName, ref)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic(err.Error())
		}
		l.PushString(schemaName)
		return 1
	}
}

var functions = map[string]func(client *DatabricksClient) lua.Function{
	"create_schema":           createSchema,
	"register_external_table": registerExternalTable,
}

func newDatabricks(ctx context.Context) lua.Function {
	return func(l *lua.State) int {
		workspaceClient := newDatabricksClient(l)
		client := &DatabricksClient{workspaceClient: workspaceClient, ctx: ctx}
		l.NewTable()
		for name, goFn := range functions {
			l.PushGoFunction(goFn(client))
			l.SetField(-2, name)
		}
		return 1
	}
}
