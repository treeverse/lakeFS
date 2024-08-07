package aws

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Shopify/go-lua"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/mitchellh/mapstructure"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
)

func newGlueClient(ctx context.Context) lua.Function {
	return func(l *lua.State) int {
		accessKeyID := lua.CheckString(l, 1)
		secretAccessKey := lua.CheckString(l, 2)
		var region string
		if !l.IsNone(3) {
			region = lua.CheckString(l, 3)
		}
		var endpoint string
		if !l.IsNone(4) {
			endpoint = lua.CheckString(l, 4)
		}
		c := &GlueClient{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			Endpoint:        endpoint,
			Region:          region,
			ctx:             ctx,
		}

		l.NewTable()
		for name, goFn := range glueFunctions {
			// -1: tbl
			l.PushGoFunction(goFn(c))
			// -1: fn, -2:tbl
			l.SetField(-2, name)
		}

		return 1
	}
}

type GlueClient struct {
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string
	Region          string
	ctx             context.Context
}

var glueFunctions = map[string]func(client *GlueClient) lua.Function{
	"get_table":       getTable,
	"create_table":    createTable,
	"update_table":    updateTable,
	"delete_table":    deleteTable,
	"create_database": createDatabase,
	"delete_database": deleteDatabase,
}

func (c *GlueClient) client() *glue.Client {
	cfg, err := config.LoadDefaultConfig(c.ctx,
		config.WithRegion(c.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, "")),
	)
	if err != nil {
		panic(err)
	}
	return glue.NewFromConfig(cfg, func(o *glue.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
}

func deleteTable(c *GlueClient) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		database := lua.CheckString(l, 1)
		tableName := lua.CheckString(l, 2)
		// check if catalog ID provided
		var catalogID *string
		if !l.IsNone(3) {
			catalogID = aws.String(lua.CheckString(l, 3))
		}
		_, err := client.DeleteTable(c.ctx, &glue.DeleteTableInput{
			DatabaseName: aws.String(database),
			Name:         aws.String(tableName),
			CatalogId:    catalogID,
		})
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}

		return 0
	}
}

func updateTable(c *GlueClient) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		database := lua.CheckString(l, 1)
		tableInputJSON := lua.CheckString(l, 2)

		// check if catalog ID provided
		var catalogID *string
		if !l.IsNone(3) {
			catalogID = aws.String(lua.CheckString(l, 3))
		}

		// version Id optional
		var versionID *string
		if !l.IsNone(4) {
			versionID = aws.String(lua.CheckString(l, 4))
		}

		// glue skip-archive optional
		var skipArchive *bool
		if !l.IsNone(5) {
			lua.CheckType(l, 5, lua.TypeBoolean)
			skipArchive = aws.Bool(l.ToBoolean(5))
		}

		// parse table input JSON
		var tableInput types.TableInput
		err := json.Unmarshal([]byte(tableInputJSON), &tableInput)
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}

		_, err = client.UpdateTable(c.ctx, &glue.UpdateTableInput{
			DatabaseName: &database,
			TableInput:   &tableInput,
			CatalogId:    catalogID,
			VersionId:    versionID,
			SkipArchive:  skipArchive,
		})

		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
		return 0
	}
}

func createTable(c *GlueClient) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		database := lua.CheckString(l, 1)
		tableInputJSON := lua.CheckString(l, 2)
		// parse table input JSON
		var tableInput types.TableInput
		err := json.Unmarshal([]byte(tableInputJSON), &tableInput)
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
		// check if catalog ID provided
		var catalogID *string
		if !l.IsNone(3) {
			catalogID = aws.String(lua.CheckString(l, 3))
		}
		// TODO(isan) Additional input params: partition index and iceberg table format
		// AWS API call
		_, err = client.CreateTable(c.ctx, &glue.CreateTableInput{
			DatabaseName: aws.String(database),
			TableInput:   &tableInput,
			CatalogId:    catalogID,
		})
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
		return 0
	}
}

func getTable(c *GlueClient) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		database := lua.CheckString(l, 1)
		table := lua.CheckString(l, 2)
		var catalogID *string
		if !l.IsNone(3) {
			catalogID = aws.String(lua.CheckString(l, 3))
		}
		resp, err := client.GetTable(c.ctx, &glue.GetTableInput{
			DatabaseName: aws.String(database),
			Name:         aws.String(table),
			CatalogId:    catalogID,
		})
		if err != nil {
			var notFoundErr *types.EntityNotFoundException
			if errors.As(err, &notFoundErr) {
				l.PushString("")
				l.PushBoolean(false) // exists
				return 2
			}
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
		// Marshal the GetTableOutput struct to JSON.
		jsonBytes, err := json.Marshal(resp)
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
		// Unmarshal the JSON to a map.
		var itemMap map[string]interface{}
		err = json.Unmarshal(jsonBytes, &itemMap)
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
		util.DeepPush(l, itemMap)
		l.PushBoolean(true)
		return 2
	}
}

type glueCreateDatabaseOpts struct {
	CreateDBInput        *glue.CreateDatabaseInput `json:"create_db_input" mapstructure:"create_db_input"`
	ErrorOnAlreadyExists bool                      `json:"error_on_already_exists" mapstructure:"error_on_already_exists"`
}

func createDatabase(c *GlueClient) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		database := lua.CheckString(l, 1)

		createDBOpts := &glueCreateDatabaseOpts{
			CreateDBInput: &glue.CreateDatabaseInput{
				DatabaseInput: &types.DatabaseInput{
					Name: aws.String(database),
				},
			},
			ErrorOnAlreadyExists: true,
		}
		// decode opts if provided and override defaults
		if !l.IsNoneOrNil(2) {
			var t interface{}
			t, err := util.PullTable(l, 2)
			if err != nil {
				lua.Errorf(l, "PullTable: %s", err.Error())
				panic("unreachable")
			}
			err = mapstructure.Decode(t, createDBOpts)
			if err != nil {
				lua.Errorf(l, "DecodeTable: %s", err.Error())
				panic("unreachable")
			}
		}

		if *createDBOpts.CreateDBInput.DatabaseInput.Name != database {
			lua.Errorf(l, "database name in input (%s) doesn't match database name parameter (%s)", *createDBOpts.CreateDBInput.DatabaseInput.Name, database)
			panic("unreachable")
		}

		// AWS API call
		_, err := client.CreateDatabase(c.ctx, createDBOpts.CreateDBInput)
		if err != nil {
			var errExists *types.AlreadyExistsException
			if !createDBOpts.ErrorOnAlreadyExists && errors.As(err, &errExists) {
				return 0
			}
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
		return 0
	}
}

func deleteDatabase(c *GlueClient) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		database := aws.String(lua.CheckString(l, 1))

		// check if catalog ID provided
		var catalogID *string
		if !l.IsNone(2) {
			catalogID = aws.String(lua.CheckString(l, 2))
		}

		_, err := client.DeleteDatabase(c.ctx, &glue.DeleteDatabaseInput{
			Name:      database,
			CatalogId: catalogID,
		})
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}
		return 0
	}
}
