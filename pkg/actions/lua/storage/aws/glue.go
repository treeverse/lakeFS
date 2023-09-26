package aws

import (
	"context"
	"encoding/json"

	"github.com/Shopify/go-lua"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
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
	"get_table":    getTable,
	"create_table": createTable,
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

func createTable(c *GlueClient) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		database := lua.CheckString(l, 1)
		tableInputStr := lua.CheckString(l, 1)

		var tableInput types.TableInput
		err := json.Unmarshal([]byte(tableInputStr), &tableInput)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}

		resp, err := client.CreateTable(c.ctx, &glue.CreateTableInput{
			DatabaseName: aws.String(database),
			TableInput:   &tableInput,
		})

		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		return util.DeepPush(l, resp.ResultMetadata)
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
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		// Marshal the GetTableOutput struct to JSON.
		jsonBytes, err := json.Marshal(resp)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		// Unmarshal the JSON to a map.
		var itemMap map[string]interface{}
		err = json.Unmarshal(jsonBytes, &itemMap)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}

		return util.DeepPush(l, itemMap)
	}
}
