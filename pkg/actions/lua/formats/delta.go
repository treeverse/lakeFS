package formats

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/aws/aws-sdk-go-v2/aws"
	delta "github.com/csimplestring/delta-go"
	"github.com/csimplestring/delta-go/storage"
	deltaStore "github.com/csimplestring/delta-go/store"
	luautil "github.com/treeverse/lakefs/pkg/actions/lua/util"
)

type storageType string

const (
	s3StorageType storageType = "s3"
)

type DeltaClient struct {
	accessProvider AccessProvider
	ctx            context.Context
}

func (dc *DeltaClient) fetchS3Table(repo, ref, prefix string, awsProps *storage.AWSProperties) (map[int64][]string, error) {
	table, err := dc.getS3DeltaTable(repo, ref, prefix, awsProps)
	if err != nil {
		return nil, err
	}
	return dc.buildLog(table)
}
func (dc *DeltaClient) getS3DeltaTable(repo, ref, prefix string, awsProps *storage.AWSProperties) (delta.Log, error) {
	config := delta.Config{StoreType: string(s3StorageType)}
	u := fmt.Sprintf("lakefs://%s/%s/%s", repo, ref, prefix)
	parsedURL, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	s3LogStore, err := deltaStore.NewS3CompatLogStore(awsProps, parsedURL)
	if err != nil {
		return nil, err
	}
	store := deltaStore.Store(s3LogStore)
	return delta.ForTableWithStore(u, config, &delta.SystemClock{}, &store)
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
			return nil, err
		}
		for _, a := range actions {
			aj, _ := a.Json()
			strLog = append(strLog, aj)
		}
		entries[entryVersion] = strLog
	}
	return entries, nil
}

func (dc *DeltaClient) fetchTableLog(repo, ref, prefix string) (map[int64][]string, error) {
	ap, _ := dc.accessProvider.GetAccessProperties()
	switch access := ap.(type) {
	case AWSInfo:
		return dc.fetchS3Table(repo, ref, prefix, &access.AWSProps)
	default:
		return nil, errors.New("unimplemented provider")
	}

}

func getTable(client *DeltaClient) lua.Function {
	return func(l *lua.State) int {
		repo := lua.CheckString(l, 1)
		ref := lua.CheckString(l, 2)
		prefix := lua.CheckString(l, 3)
		tableLog, err := client.fetchTableLog(repo, ref, prefix)
		if err != nil {
			lua.Errorf(l, err.Error())
			panic("failed fetching table log")
		}
		luautil.DeepPush(l, tableLog)
		return 1
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
func newDelta(ctx context.Context, serverAddress string) lua.Function {
	if strings.HasPrefix(serverAddress, ":") {
		// workaround in case we listen on all interfaces without specifying ip
		serverAddress = fmt.Sprintf("localhost%s", serverAddress)
	}
	serverAddress = fmt.Sprintf("http://%s", serverAddress)
	return func(l *lua.State) int {
		var client *DeltaClient
		st := lua.CheckString(l, 1)
		switch storageType(st) {
		case s3StorageType:
			client = newS3DeltaClient(l, ctx, serverAddress)
		default:
			lua.Errorf(l, "unimplemented storage type")
			panic("unimplemented storage type")
		}
		l.NewTable()
		for name, goFn := range functions {
			l.PushGoFunction(goFn(client))
			l.SetField(-2, name)
		}
		return 1
	}
}

func newS3DeltaClient(l *lua.State, ctx context.Context, serverAddress string) *DeltaClient {
	accessKeyID := lua.CheckString(l, 2)
	secretAccessKey := lua.CheckString(l, 3)
	r := lua.CheckString(l, 4)
	awsProps := storage.AWSProperties{
		Region:         r,
		ForcePathStyle: true,
		CredsProvider: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
			}, nil
		}),
		Endpoint: serverAddress,
	}
	storage.RegisterS3CompatBucketURLOpener("lakefs", &awsProps)

	return &DeltaClient{accessProvider: AWSInfo{AWSProps: awsProps}, ctx: ctx}
}
