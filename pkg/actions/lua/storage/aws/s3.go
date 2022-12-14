package aws

import (
	"context"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/Shopify/go-lua"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
)

func Open(l *lua.State, ctx context.Context) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "s3_client", Function: newS3Client(ctx)},
		})
		return 1
	}
	lua.Require(l, "aws", open, false)
	l.Pop(1)
}

func newS3Client(ctx context.Context) lua.Function {
	return func(l *lua.State) int {
		var accessKeyID, secretAccessKey, endpoint, region string
		accessKeyID = lua.CheckString(l, 1)
		secretAccessKey = lua.CheckString(l, 2)
		if !l.IsNone(3) {
			region = lua.CheckString(l, 3)
		}
		if !l.IsNone(4) {
			endpoint = lua.CheckString(l, 4)
		}
		c := &S3Client{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			Endpoint:        endpoint,
			Region:          region, ctx: ctx,
		}

		l.NewTable()
		for name, goFn := range regexpFunc {
			// -1: tbl
			l.PushGoFunction(goFn(c))
			// -1: fn, -2:tbl
			l.SetField(-2, name)
		}

		return 1
	}
}

type S3Client struct {
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string
	Region          string
	ctx             context.Context
}

func (c *S3Client) client() *s3.S3 {
	cfg := &aws.Config{}
	if c.Region != "" {
		cfg.Region = aws.String(c.Region)
	}
	if c.Endpoint != "" {
		cfg.Endpoint = aws.String(c.Endpoint)
	}
	cfg.Credentials = credentials.NewStaticCredentials(c.AccessKeyID, c.SecretAccessKey, "")
	sess, _ := session.NewSession(cfg)
	return s3.New(sess)
}

var regexpFunc = map[string]func(client *S3Client) lua.Function{
	"get_object":       getObject,
	"put_object":       putObject,
	"list_objects":     listObjects,
	"delete_object":    deleteObject,
	"delete_recursive": deleteRecursive,
}

func deleteRecursive(c *S3Client) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		iter := s3manager.NewDeleteListIterator(client, &s3.ListObjectsInput{
			Bucket: aws.String(lua.CheckString(l, 1)),
			Prefix: aws.String(lua.CheckString(l, 2)),
		})
		err := s3manager.NewBatchDeleteWithClient(client).Delete(c.ctx, iter)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		return 0
	}
}

func getObject(c *S3Client) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		key := lua.CheckString(l, 2)
		bucket := lua.CheckString(l, 1)
		resp, err := client.GetObjectWithContext(c.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey:
					l.PushString("")
					l.PushBoolean(false) // exists
					return 2
				}
			}
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		l.PushString(string(data))
		l.PushBoolean(true) // exists
		return 2
	}
}

func putObject(c *S3Client) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		buf := strings.NewReader(lua.CheckString(l, 3))
		_, err := client.PutObjectWithContext(c.ctx, &s3.PutObjectInput{
			Body:   buf,
			Bucket: aws.String(lua.CheckString(l, 1)),
			Key:    aws.String(lua.CheckString(l, 2)),
		})
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		return 0
	}
}

func deleteObject(c *S3Client) lua.Function {
	return func(l *lua.State) int {
		client := c.client()
		_, err := client.DeleteObjectWithContext(c.ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(lua.CheckString(l, 1)),
			Key:    aws.String(lua.CheckString(l, 2)),
		})
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		return 0
	}
}

func listObjects(c *S3Client) lua.Function {
	return func(l *lua.State) int {
		client := c.client()

		var prefix, delimiter, continuationToken *string
		if !l.IsNone(2) {
			prefix = aws.String(lua.CheckString(l, 2))
		}
		if !l.IsNone(3) {
			continuationToken = aws.String(lua.CheckString(l, 3))
		}
		if !l.IsNone(4) {
			delimiter = aws.String(lua.CheckString(l, 4))
		} else {
			delimiter = aws.String("/")
		}

		resp, err := client.ListObjectsV2WithContext(c.ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(lua.CheckString(l, 1)),
			ContinuationToken: continuationToken,
			Delimiter:         delimiter,
			Prefix:            prefix,
		})
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("unreachable")
		}
		results := make([]map[string]interface{}, 0)
		for _, prefix := range resp.CommonPrefixes {
			results = append(results, map[string]interface{}{
				"key":  *prefix.Prefix,
				"type": "prefix",
			})
		}
		for _, obj := range resp.Contents {
			results = append(results, map[string]interface{}{
				"key":           *obj.Key,
				"type":          "object",
				"etag":          *obj.ETag,
				"size":          *obj.Size,
				"last_modified": obj.LastModified.Format(time.RFC3339),
			})
		}

		// sort it
		sort.Slice(results, func(i, j int) bool {
			return results[i]["key"].(string) > results[j]["key"].(string)
		})

		response := map[string]interface{}{
			"is_truncated":            aws.BoolValue(resp.IsTruncated),
			"next_continuation_token": aws.StringValue(resp.NextContinuationToken),
			"results":                 results,
		}

		return util.DeepPush(l, response)
	}
}
