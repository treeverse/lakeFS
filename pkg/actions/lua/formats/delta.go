package formats

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/Shopify/go-lua"
	"github.com/aws/aws-sdk-go-v2/aws"
	delta "github.com/csimplestring/delta-go"
	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/storage"
	luautil "github.com/treeverse/lakefs/pkg/actions/lua/util"
	"gocloud.dev/blob"
)

type storageType string

const (
	s3StorageType storageType = "s3"
)

var ErrUnimplementedProvided = errors.New("unimplemented provider")

type DeltaClient struct {
	accessProvider AccessProvider
	ctx            context.Context
	mux            blob.URLMux
}

func newDeltaTableMetadata(meta *action.Metadata) map[string]any {
	return map[string]any{
		"description":       meta.Description,
		"id":                meta.ID,
		"name":              meta.Name,
		"schema_string":     meta.SchemaString,
		"partition_columns": meta.PartitionColumns,
		"configuration":     meta.Configuration,
		"created_time":      *meta.CreatedTime,
	}
}

func (dc *DeltaClient) fetchS3Table(repo, ref, prefix string) (map[int64][]string, map[string]any, error) {
	table, err := dc.getS3DeltaTable(repo, ref, prefix)
	if err != nil {
		return nil, nil, err
	}
	log, err := dc.buildLog(table)
	if err != nil {
		return nil, nil, err
	}
	meta, err := dc.getTableMetadata(table)
	if err != nil {
		return nil, nil, err
	}
	return log, meta, nil
}

func (dc *DeltaClient) getTableMetadata(log delta.Log) (map[string]any, error) {
	s, err := log.Snapshot()
	if err != nil {
		return nil, err
	}
	m, err := s.Metadata()
	if err != nil {
		return nil, err
	}
	return newDeltaTableMetadata(m), nil
}

func (dc *DeltaClient) getS3DeltaTable(repo, ref, prefix string) (delta.Log, error) {
	config := delta.Config{StoreType: string(s3StorageType)}
	u := fmt.Sprintf("lakefs://%s/%s/%s", repo, ref, prefix)
	return delta.ForTableWithMux(u, config, &delta.SystemClock{}, &dc.mux)
}

func (dc *DeltaClient) buildLog(table delta.Log) (map[int64][]string, error) {
	s, err := table.Snapshot()
	if err != nil {
		return nil, err
	}
	version, err := s.EarliestVersion()
	if err != nil {
		return nil, err
	}
	versionLog, err := table.Changes(version, false)
	if err != nil {
		return nil, err
	}

	entries := make(map[int64][]string)
	for entry, err := versionLog.Next(); err == nil; entry, err = versionLog.Next() {
		strLog := make([]string, 0)
		entryVersion := entry.Version()
		actions, aErr := entry.Actions()
		if aErr != nil {
			return nil, aErr
		}
		for _, a := range actions {
			aj, _ := a.Json()
			strLog = append(strLog, aj)
		}
		entries[entryVersion] = strLog
	}
	return entries, nil
}

func (dc *DeltaClient) fetchTableLog(repo, ref, prefix string) (map[int64][]string, map[string]any, error) {
	ap, _ := dc.accessProvider.GetAccessProperties()
	switch ap.(type) {
	case AWSInfo:
		return dc.fetchS3Table(repo, ref, prefix)
	default:
		return nil, nil, ErrUnimplementedProvided
	}
}

func getTable(client *DeltaClient) lua.Function {
	return func(l *lua.State) int {
		repo := lua.CheckString(l, 1)
		ref := lua.CheckString(l, 2)
		prefix := lua.CheckString(l, 3)
		tableLog, metadata, err := client.fetchTableLog(repo, ref, prefix)
		if err != nil {
			lua.Errorf(l, "%s", err.Error())
			panic("failed fetching table log")
		}
		luautil.DeepPush(l, tableLog)
		luautil.DeepPush(l, metadata)
		return 2
	}
}

var functions = map[string]func(client *DeltaClient) lua.Function{
	"get_table": getTable,
}

// AccessProvider is used to provide different expected access properties to different storage providers
type AccessProvider interface {
	GetAccessProperties() (interface{}, error)
}

type AWSInfo struct {
	AWSProps storage.AWSProperties
}

func (awsI AWSInfo) GetAccessProperties() (interface{}, error) {
	return awsI, nil
}

// newDelta is a factory function to create server/cloud specific Delta Lake client
// lakeFSAddr is the domain or "authority:port" of the running lakeFS server
func newDelta(ctx context.Context, lakeFSAddr string) lua.Function {
	if regexp.MustCompile(`^:\d+`).MatchString(lakeFSAddr) {
		// workaround in case we listen on all interfaces without specifying ip
		lakeFSAddr = fmt.Sprintf("localhost%s", lakeFSAddr)
	}
	lakeFSAddr = fmt.Sprintf("http://%s", lakeFSAddr)
	return func(l *lua.State) int {
		client := newS3DeltaClient(l, ctx, lakeFSAddr)
		l.NewTable()
		for name, goFn := range functions {
			l.PushGoFunction(goFn(client))
			l.SetField(-2, name)
		}
		return 1
	}
}

func newS3DeltaClient(l *lua.State, ctx context.Context, lakeFSAddr string) *DeltaClient {
	accessKeyID := lua.CheckString(l, 1)
	secretAccessKey := lua.CheckString(l, 2)
	awsProps := storage.AWSProperties{
		ForcePathStyle: true,
		CredsProvider: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
			}, nil
		}),
		Endpoint: lakeFSAddr,
	}
	if !l.IsNone(3) {
		awsProps.Region = lua.CheckString(l, 3)
	}

	client := &DeltaClient{accessProvider: AWSInfo{AWSProps: awsProps}, ctx: ctx}
	storage.RegisterS3CompatBucketURLOpener("lakefs", &awsProps, &client.mux)
	return client
}
