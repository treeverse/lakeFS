package services

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/go-lua"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
	"strings"
)

var ErrExpectedMetatable = errors.New("expected a metadata table")

type DatabricksClient struct {
	workspaceClient *databricks.WorkspaceClient
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

type field struct {
	Name     string
	Type     string
	Nullable bool
}

func newField(fs map[string]any) field {
	name := fs["name"].(string)
	t := fs["type"].(string)
	nullable := fs["nullable"].(bool)
	return field{Name: name, Type: t, Nullable: nullable}
}

type schema struct {
	Name   string
	Type   string
	Fields []field
}

func newSchema(s interface{}) schema {
	schemaMap := s.(map[string]any)
	name := schemaMap["name"].(string)
	t := schemaMap["type"].(string)
	fs := schemaMap["fields"].([]map[string]any)
	fields := make([]field, 0, len(fs))
	for _, f := range fs {
		fields = append(fields, newField(f))
	}
	return schema{Name: name, Type: t, Fields: fields}
}

func generateColumnsDef(s schema) string {
	columnDefArr := make([]string, 0)
	for _, f := range s.Fields {
		column := fmt.Sprintf("%s %s", f.Name, f.Type)
		if !f.Nullable {
			column = fmt.Sprintf("%s NOT NULL", column)
		}
		columnDefArr = append(columnDefArr, column)
	}
	columnDefArr = append(columnDefArr, ")")
	return fmt.Sprintf("(%s)", strings.Join(columnDefArr, ", "))
}

func generateTableCreationStatement(sc schema, metadata any, tableName string, location string) (string, error) {
	tableDefStr := generateColumnsDef(sc)
	var comment string
	var tblprops string
	metamap, ok := metadata.(map[string]any)
	if !ok {
		return "", ErrExpectedMetatable
	} else {
		c, ok := metamap["comment"]
		if ok {
			cs, ok := c.(string)
			if ok {
				comment = fmt.Sprintf("'%s'", cs)
			}
		}
		tp, ok := metamap["tblproperties"]
		if ok {
			tpsm, ok := tp.(map[string]string)
			if ok {
				props := make([]string, 0, len(tpsm))
				for k, v := range tpsm {
					props = append(props, fmt.Sprintf("'%s' = '%s'", k, v))
				}
				tblprops = fmt.Sprintf("(%s)", strings.Join(props, ", "))
			}
		}
	}
	statement := fmt.Sprintf(`CREATE EXTERNAL TABLE %s %s LOCATION '%s'`, tableName, tableDefStr, location)
	if comment != "" {
		statement = fmt.Sprintf("%s COMMENT %s", statement, comment)
	}
	if tblprops != "" {
		statement = fmt.Sprintf("%s TBLPROPERTIES %s", statement, tblprops)
	}
	return statement, nil
}

func registerExternalTable(client *DatabricksClient) lua.Function {
	return func(l *lua.State) int {
		tableName := lua.CheckString(l, 1)
		location := lua.CheckString(l, 2)
		s, err := util.PullTable(l, 3)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("failed fetching table schema definition")
		}
		metadata, err := util.PullTable(l, 4)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("failed fetching table metadata")
		}
		warehouseId := lua.CheckString(l, 5)
		catalogName := lua.CheckString(l, 6)

		sc := newSchema(s)
		statement, err := generateTableCreationStatement(sc, metadata, tableName, location)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("failed creating table statement")
		}
		esr, err := client.workspaceClient.StatementExecution.ExecuteAndWait(context.Background(), sql.ExecuteStatementRequest{
			WarehouseId: warehouseId,
			Catalog:     catalogName,
			Schema:      sc.Name,
			Statement:   statement,
		})
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("failed creating table")
		}
		l.PushString(esr.Status.State.String())
		return 1
	}
}

var functions = map[string]func(client *DatabricksClient) lua.Function{
	"register_external_table": registerExternalTable,
}

func newDatabricks() lua.Function {
	return func(l *lua.State) int {
		workspaceClient := newDatabricksClient(l)
		client := &DatabricksClient{workspaceClient: workspaceClient}
		l.NewTable()
		for name, goFn := range functions {
			l.PushGoFunction(goFn(client))
			l.SetField(-2, name)
		}
		return 1
	}
}
