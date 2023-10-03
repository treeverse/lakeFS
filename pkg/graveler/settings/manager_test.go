package settings_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/testutil"
)

type mockCache struct {
	c map[interface{}]interface{}
}

var repository = &graveler.RepositoryRecord{
	RepositoryID: "example-repo",
	Repository: &graveler.Repository{
		StorageNamespace: "mem://my-storage",
		DefaultBranchID:  "main",
	},
}

func (m *mockCache) GetOrSet(k interface{}, setFn cache.SetFn) (v interface{}, err error) {
	if val, ok := m.c[k]; ok {
		return val, nil
	}
	val, err := setFn()
	if err != nil {
		return nil, err
	}
	m.c[k] = val
	return val, nil
}

func TestSaveAndGet(t *testing.T) {
	ctx := context.Background()
	mc := &mockCache{
		c: make(map[interface{}]interface{}),
	}
	m, _ := prepareTest(t, ctx, mc, nil)
	firstSettings := &settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}
	err := m.Save(ctx, repository, "settingKey", firstSettings, nil)
	testutil.Must(t, err)
	gotSettings := &settings.ExampleSettings{}
	err = m.Get(ctx, repository, "settingKey", gotSettings)
	testutil.Must(t, err)
	if diff := deep.Equal(firstSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	secondSettings := &settings.ExampleSettings{ExampleInt: 15, ExampleStr: "hi", ExampleMap: map[string]int32{"boo": 16}}
	err = m.Save(ctx, repository, "settingKey", secondSettings, nil)
	testutil.Must(t, err)
	// the result should be cached, and we should get the first settings:
	gotSettings = &settings.ExampleSettings{}
	err = m.Get(ctx, repository, "settingKey", gotSettings)
	testutil.Must(t, err)
	if diff := deep.Equal(firstSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	// after clearing the mc, we should get the new settings:
	mc.c = make(map[interface{}]interface{})
	gotSettings = &settings.ExampleSettings{}
	err = m.Get(ctx, repository, "settingKey", gotSettings)
	testutil.Must(t, err)
	if diff := deep.Equal(secondSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
}
func TestGetLatest(t *testing.T) {
	ctx := context.Background()
	m, _ := prepareTest(t, ctx, nil, nil)
	setting := &settings.ExampleSettings{}
	_, err := m.GetLatest(ctx, repository, "settingKey", setting)
	if !errors.Is(err, graveler.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	err = m.Save(ctx, repository, "settingKey", &settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}, nil)
	testutil.Must(t, err)
	setting = &settings.ExampleSettings{}
	eTag, err := m.GetLatest(ctx, repository, "settingKey", setting)
	testutil.Must(t, err)
	if diff := deep.Equal(&settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}, setting); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	if eTag == nil || *eTag == "" {
		t.Fatal("got empty eTag")
	}
}

func TestConditionalSave(t *testing.T) {
	ctx := context.Background()
	mc := &mockCache{
		c: make(map[interface{}]interface{}),
	}
	m, _ := prepareTest(t, ctx, mc, nil)
	firstSettings := &settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}
	err := m.Save(ctx, repository, "settingKey", firstSettings, swag.String(""))
	testutil.Must(t, err)
	gotSettings := &settings.ExampleSettings{}
	checksum, err := m.GetLatest(ctx, repository, "settingKey", gotSettings)
	testutil.Must(t, err)
	if diff := deep.Equal(firstSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	secondSettings := &settings.ExampleSettings{ExampleInt: 15, ExampleStr: "hi", ExampleMap: map[string]int32{"boo": 16}}
	err = m.Save(ctx, repository, "settingKey", secondSettings, checksum)
	testutil.Must(t, err)
	err = m.Save(ctx, repository, "settingKey", secondSettings, swag.String("WRONG_CHECKSUM"))
	if !errors.Is(err, graveler.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}
}

func prepareTest(t *testing.T, ctx context.Context, refCache cache.Cache, branchLockCallback func(context.Context, *graveler.RepositoryRecord, graveler.BranchID, func() (interface{}, error)) (interface{}, error)) (*settings.Manager, block.Adapter) {
	ctrl := gomock.NewController(t)
	refManager := mock.NewMockRefManager(ctrl)

	blockAdapter := mem.New(context.Background())
	branchLock := mock.NewMockBranchLocker(ctrl)
	cb := func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f func() (interface{}, error)) (interface{}, error) {
		return f()
	}
	if branchLockCallback != nil {
		cb = branchLockCallback
	}
	var opts []settings.ManagerOption
	if refCache == nil {
		refCache = cache.NoCache
	}
	opts = append(opts, settings.WithCache(refCache))
	branchLock.EXPECT().MetadataUpdater(ctx, gomock.Eq(repository), graveler.BranchID("main"), gomock.Any()).DoAndReturn(cb).AnyTimes()
	kvStore := kvtest.GetStore(ctx, t)
	m := settings.NewManager(refManager, kvStore, opts...)

	refManager.EXPECT().GetRepository(ctx, gomock.Eq(repository.RepositoryID)).AnyTimes().Return(repository, nil)
	return m, blockAdapter
}
