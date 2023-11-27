package formats

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/go-lua"
	"github.com/aws/aws-sdk-go-v2/aws"
	delta "github.com/csimplestring/delta-go"
	deltaStore "github.com/csimplestring/delta-go/store"
	luautil "github.com/treeverse/lakefs/pkg/actions/lua/util"
	"path"
)

type storageType string

const (
	s3StorageType    storageType = "s3"
	gcsStorageType   storageType = "gcs"
	azureStorageType storageType = "azure"
)

type DeltaClient struct {
	accessProvider AccessProvider
	ctx            context.Context
}

func (dc *DeltaClient) s3Fetcher(repo, ref, prefix string) (map[int64][]string, error) {
	table, err := dc.buildS3DeltaTable(repo, ref, prefix)
	if err != nil {
		return nil, err
	}
	return dc.buildLog(table)
}
func (dc *DeltaClient) buildS3DeltaTable(repo, ref, prefix string) (delta.Log, error) {
	awsInfo := dc.accessProvider.(AWSInfo)
	config := delta.Config{StoreType: string(s3StorageType)}
	s3LogStore, err := deltaStore.NewS3CompatLogStore(&awsInfo.AWSProps, repo, path.Join(ref, prefix))
	if err != nil {
		return nil, err
	}
	store := deltaStore.Store(s3LogStore)
	url := fmt.Sprintf("lakefs://%s/%s/%s", repo, ref, prefix)
	return delta.ForTableWithStore(url, config, &delta.SystemClock{}, &store)
}
func (dc *DeltaClient) buildLog(table delta.Log) (map[int64][]string, error) {
	s, err := table.Snapshot()
	if err != nil {
		return nil, err
	}
	version, _ := s.EarliestVersion()
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
	switch ap.(type) {
	case AWSInfo:
		return dc.s3Fetcher(repo, ref, prefix)
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

func fetchFormattedChanges(client *DeltaClient) lua.Function {
	return func(l *lua.State) int {
		return 1
	}
}

var functions = map[string]func(client *DeltaClient) lua.Function{
	"get_table":               getTable,
	"fetch_formatted_changes": fetchFormattedChanges,
}

type AccessProvider interface {
	GetAccessProperties() (interface{}, error)
}

type AWSInfo struct {
	AWSProps deltaStore.AWSProperties
}

type AWSInfo2 struct {
	Region     string
	DisableSsl bool
	Key        string
	Secret     string
	Endpoint   string
}

func (awsI AWSInfo) GetAccessProperties() (interface{}, error) {
	return awsI, nil
}

func newDelta(ctx context.Context) lua.Function {
	// Factory method to create storage specific Delta Lake client
	return func(l *lua.State) int {
		var client *DeltaClient
		st := lua.CheckString(l, 1)
		switch storageType(st) {
		case s3StorageType:
			client = newS3DeltaClient(l, ctx)
		default:
			lua.Errorf(l, "unimplemented storage type")
			panic("unimplemented storage type")
		}
		l.NewTable()
		for name, goFn := range functions {
			// -1: tbl
			l.PushGoFunction(goFn(client))
			// -1: fn, -2:tbl
			l.SetField(-2, name)
		}
		return 1
	}
}

func newS3DeltaClient(l *lua.State, ctx context.Context) *DeltaClient {
	aki := lua.CheckString(l, 2)
	sak := lua.CheckString(l, 3)
	e := lua.CheckString(l, 4)
	r := lua.OptString(l, 5, "us-east-1")
	disableSsl := lua.CheckInteger(l, 6) == 1
	awsProps := deltaStore.AWSProperties{
		Region:         r,
		ForcePathStyle: true,
		DisableHTTPs:   disableSsl,
		CredsProvider: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     aki,
				SecretAccessKey: sak,
			}, nil
		}),
		Endpoint: e,
	}
	deltaStore.RegisterS3CompatBucketURLOpener("lakefs", &awsProps)

	return &DeltaClient{accessProvider: AWSInfo{AWSProps: awsProps}, ctx: ctx}
}
