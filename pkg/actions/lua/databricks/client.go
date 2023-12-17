package databricks

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

var r = regexp.MustCompile(`\\W`)

var (
	ErrInvalidTableName       = errors.New("invalid table name")
	ErrInvalidTableNameLength = errors.New("invalid table name length")
)

type Client struct {
	workspaceClient *databricks.WorkspaceClient
	ctx             context.Context
}

// https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
// https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html
func validateTableName(tableName string) error {
	if r.Match([]byte(tableName)) {
		return ErrInvalidTableName
	} else if len(tableName) > 255 {
		return ErrInvalidTableNameLength
	}
	return nil
}

func validateTableLocation(tableLocation string) error {
	_, err := url.ParseRequestURI(tableLocation)
	return err
}

func validateTableInput(tableName, location string) error {
	errName := validateTableName(tableName)
	errLocation := validateTableLocation(location)
	return errors.Join(errName, errLocation)
}

func (dbc *Client) createExternalTable(warehouseID, catalogName, schemaName, tableName, location string) (string, error) {
	if err := validateTableInput(tableName, location); err != nil {
		return "", err
	}
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

func tableFullName(catalogName, schemaName, tableName string) string {
	return fmt.Sprintf("%s.%s.%s", catalogName, schemaName, tableName)
}

func (dbc *Client) dropTable(catalogName, schemaName, tableName string) error {
	return dbc.workspaceClient.Tables.DeleteByFullName(dbc.ctx, tableFullName(catalogName, schemaName, tableName))
}

func (dbc *Client) createOrGetSchema(catalogName, schemaName string) (*catalog.SchemaInfo, error) {
	schemaInfo, err := dbc.workspaceClient.Schemas.Create(dbc.ctx, catalog.CreateSchema{
		Name:        schemaName,
		CatalogName: catalogName,
	})
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// Full name of schema, in form of <catalog_name>.<schema_name>
			schemaInfo, err = dbc.workspaceClient.Schemas.GetByFullName(dbc.ctx, catalogName+"."+schemaName)
			if err != nil {
				return nil, err
			}
			return schemaInfo, nil
		}
		return nil, err
	}
	return schemaInfo, nil
}

func newDatabricksClient(l *lua.State) (*databricks.WorkspaceClient, error) {
	host := lua.CheckString(l, -2)
	token := lua.CheckString(l, -1)
	wsc, err := databricks.NewWorkspaceClient(
		&databricks.Config{
			Host:        host,
			Token:       token,
			Credentials: config.PatCredentials{},
		},
	)
	if err != nil {
		return nil, err
	}
	return wsc, err
}

func registerExternalTable(client *Client) lua.Function {
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

func createSchema(client *Client) lua.Function {
	return func(l *lua.State) int {
		ref := lua.CheckString(l, 1)
		catalogName := lua.CheckString(l, 2)
		schemaInfo, err := client.createOrGetSchema(catalogName, ref)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic(err.Error())
		}
		l.PushString(schemaInfo.Name)
		return 1
	}
}

var functions = map[string]func(client *Client) lua.Function{
	"create_schema":           createSchema,
	"register_external_table": registerExternalTable,
}

func newClient(ctx context.Context) lua.Function {
	return func(l *lua.State) int {
		workspaceClient, err := newDatabricksClient(l)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		client := &Client{workspaceClient: workspaceClient, ctx: ctx}
		l.NewTable()
		for name, goFn := range functions {
			l.PushGoFunction(goFn(client))
			l.SetField(-2, name)
		}
		return 1
	}
}
