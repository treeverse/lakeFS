package aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/Shopify/go-lua"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
)

var errDeleteObject = errors.New("delete object failed")

func newS3Client(ctx context.Context) lua.Function {
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

		c := &S3Client{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			Endpoint:        endpoint,
			Region:          region,
			ctx:             ctx,
		}
		l.NewTable()
		functions := map[string]lua.Function{
			"get_object":       c.GetObject,
			"put_object":       c.PutObject,
			"list_objects":     c.ListObjects,
			"delete_object":    c.DeleteObject,
			"delete_recursive": c.DeleteRecursive,
		}
		for name, goFn := range functions {
			l.PushGoFunction(goFn)
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

func (c *S3Client) client() *s3.Client {
	cfg, err := config.LoadDefaultConfig(c.ctx,
		config.WithRegion(c.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, "")),
	)
	if err != nil {
		panic(err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
}

func (c *S3Client) DeleteRecursive(l *lua.State) int {
	bucketName := lua.CheckString(l, 1)
	prefix := lua.CheckString(l, 2)

	client := c.client()
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	var errs error
	for {
		// list objects to delete and delete them
		listObjects, err := client.ListObjectsV2(c.ctx, input)
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("unreachable")
		}

		deleteInput := &s3.DeleteObjectsInput{
			Bucket: &bucketName,
			Delete: &types.Delete{},
		}
		for _, content := range listObjects.Contents {
			deleteInput.Delete.Objects = append(deleteInput.Delete.Objects, types.ObjectIdentifier{Key: content.Key})
		}
		deleteObjects, err := client.DeleteObjects(c.ctx, deleteInput)
		if err != nil {
			errs = errors.Join(errs, err)
			break
		}
		for _, deleteError := range deleteObjects.Errors {
			errDel := fmt.Errorf("%w '%s', %s",
				errDeleteObject, aws.ToString(deleteError.Key), aws.ToString(deleteError.Message))
			errs = errors.Join(errs, errDel)
		}

		if !aws.ToBool(listObjects.IsTruncated) {
			break
		}
		input.ContinuationToken = listObjects.NextContinuationToken
	}
	if errs != nil {
		lua.Errorf(l, "%s", errs.Error())
		panic("unreachable")
	}
	return 0
}

func (c *S3Client) GetObject(l *lua.State) int {
	client := c.client()
	key := lua.CheckString(l, 2)
	bucket := lua.CheckString(l, 1)
	resp, err := client.GetObject(c.ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var (
			noSuchBucket *types.NoSuchBucket
			noSuchKey    *types.NoSuchKey
		)
		if errors.As(err, &noSuchBucket) || errors.As(err, &noSuchKey) {
			l.PushString("")
			l.PushBoolean(false) // exists
			return 2
		}
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	l.PushString(string(data))
	l.PushBoolean(true) // exists
	return 2
}

func (c *S3Client) PutObject(l *lua.State) int {
	client := c.client()
	buf := strings.NewReader(lua.CheckString(l, 3))
	_, err := client.PutObject(c.ctx, &s3.PutObjectInput{
		Body:   buf,
		Bucket: aws.String(lua.CheckString(l, 1)),
		Key:    aws.String(lua.CheckString(l, 2)),
	})
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	return 0
}

func (c *S3Client) DeleteObject(l *lua.State) int {
	client := c.client()
	_, err := client.DeleteObject(c.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(lua.CheckString(l, 1)),
		Key:    aws.String(lua.CheckString(l, 2)),
	})
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	return 0
}

func (c *S3Client) ListObjects(l *lua.State) int {
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

	resp, err := client.ListObjectsV2(c.ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(lua.CheckString(l, 1)),
		ContinuationToken: continuationToken,
		Delimiter:         delimiter,
		Prefix:            prefix,
	})
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
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
			"size":          obj.Size,
			"last_modified": obj.LastModified.Format(time.RFC3339),
		})
	}

	// sort it
	sort.Slice(results, func(i, j int) bool {
		return results[i]["key"].(string) > results[j]["key"].(string)
	})

	response := map[string]interface{}{
		"is_truncated":            resp.IsTruncated,
		"next_continuation_token": aws.ToString(resp.NextContinuationToken),
		"results":                 results,
	}

	return util.DeepPush(l, response)
}
