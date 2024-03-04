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
	luautil "github.com/treeverse/lakefs/pkg/actions/lua/util"
)

// identifierRegex https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
var identifierRegex = regexp.MustCompile(`\W`)

var ErrInvalidTableName = errors.New("invalid table name")

type Client struct {
	workspaceClient *databricks.WorkspaceClient
	ctx             context.Context
}

// validateTableName
// https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
// https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html
func validateTableName(tableName string) error {
	if identifierRegex.MatchString(tableName) {
		return ErrInvalidTableName
	}
	if len(tableName) > 255 {
		return ErrInvalidTableName
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

func (client *Client) createExternalTable(warehouseID, catalogName, schemaName, tableName, location string, metadata map[string]any) (string, error) {
	if err := validateTableInput(tableName, location); err != nil {
		return "", fmt.Errorf("external table \"%s\" creation failed: %w", tableName, err)
	}
	statement := fmt.Sprintf(`CREATE EXTERNAL TABLE %s LOCATION '%s'`, tableName, location)
	if metadata != nil && metadata["description"] != "" {
		if ms, ok := metadata["description"].(string); ok {
			statement = fmt.Sprintf(`%s COMMENT '%s'`, statement, ms)
		}
	}
	esr, err := client.workspaceClient.StatementExecution.ExecuteAndWait(client.ctx, sql.ExecuteStatementRequest{
		WarehouseId: warehouseID,
		Catalog:     catalogName,
		Schema:      schemaName,
		Statement:   statement,
	})
	if err != nil {
		return "", fmt.Errorf("external table \"%s\" creation failed: %w", tableName, err)
	}
	return esr.Status.State.String(), nil
}

func tableFullName(catalogName, schemaName, tableName string) string {
	return fmt.Sprintf("%s.%s.%s", catalogName, schemaName, tableName)
}

func (client *Client) deleteTable(catalogName, schemaName, tableName string) error {
	err := client.workspaceClient.Tables.DeleteByFullName(client.ctx, tableFullName(catalogName, schemaName, tableName))
	if err != nil {
		return fmt.Errorf("failed deleting an existing table \"%s\": %w", tableName, err)
	}
	return nil
}

func (client *Client) createSchema(catalogName, schemaName string, getIfExists bool) (*catalog.SchemaInfo, error) {
	schemaInfo, err := client.workspaceClient.Schemas.Create(client.ctx, catalog.CreateSchema{
		Name:        schemaName,
		CatalogName: catalogName,
	})

	if err == nil {
		return schemaInfo, nil
	}
	if getIfExists && alreadyExists(err) {
		// Full name of schema, in form of <catalog_name>.<schema_name>
		schemaInfo, err = client.workspaceClient.Schemas.GetByFullName(client.ctx, catalogName+"."+schemaName)
		if err == nil {
			return schemaInfo, nil
		}
		return nil, fmt.Errorf("failed getting schema \"%s\": %w", schemaName, err)
	}
	return nil, fmt.Errorf("failed creating schema \"%s\": %w", schemaName, err)
}

func newDatabricksClient(l *lua.State) (*databricks.WorkspaceClient, error) {
	host := lua.CheckString(l, 1)
	token := lua.CheckString(l, 2)
	return databricks.NewWorkspaceClient(
		&databricks.Config{
			Host:        host,
			Token:       token,
			Credentials: config.PatCredentials{},
		},
	)
}

func (client *Client) RegisterExternalTable(l *lua.State) int {
	tableName := lua.CheckString(l, 1)
	tableName = strings.ReplaceAll(tableName, "-", "_")
	location := lua.CheckString(l, 2)
	warehouseID := lua.CheckString(l, 3)
	catalogName := lua.CheckString(l, 4)
	schemaName := lua.CheckString(l, 5)
	metadata, _ := luautil.PullTable(l, 6)
	var metadataMap map[string]any
	if metadata != nil {
		metadataMap = metadata.(map[string]any)
	}
	status, err := client.createExternalTable(warehouseID, catalogName, schemaName, tableName, location, metadataMap)
	if err != nil {
		if alreadyExists(err) {
			err = client.deleteTable(catalogName, schemaName, tableName)
			if err != nil {
				lua.Errorf(l, "%s", err.Error())
				panic("unreachable")
			}
			status, err = client.createExternalTable(warehouseID, catalogName, schemaName, tableName, location, metadataMap)
			if err != nil {
				lua.Errorf(l, "%s", err.Error())
				panic("unreachable")
			}
		} else {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
	}
	l.PushString(status)
	return 1
}

func (client *Client) CreateSchema(l *lua.State) int {
	ref := lua.CheckString(l, 1)
	catalogName := lua.CheckString(l, 2)
	getIfExists := l.ToBoolean(3)
	schemaInfo, err := client.createSchema(catalogName, ref, getIfExists)
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	l.PushString(schemaInfo.Name)
	return 1
}

func alreadyExists(e error) bool {
	return strings.Contains(e.Error(), "already exists")
}

func newClient(ctx context.Context) lua.Function {
	return func(l *lua.State) int {
		workspaceClient, err := newDatabricksClient(l)
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
		client := &Client{workspaceClient: workspaceClient, ctx: ctx}
		l.NewTable()
		functions := map[string]lua.Function{
			"create_schema":           client.CreateSchema,
			"register_external_table": client.RegisterExternalTable,
		}
		for name, goFn := range functions {
			l.PushGoFunction(goFn)
			l.SetField(-2, name)
		}
		return 1
	}
}
