package settings_test

import (
	"context"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/modules/cache"
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

func (m *mockCache) GetOrSetWithExpiry(k interface{}, setFn cache.SetFnWithExpiry) (v interface{}, err error) {
	// Settings does not use expiry.
	panic("Not implemented.")
}

func TestNonExistent(t *testing.T) {
	ctx := context.Background()
	m := prepareTest(t, ctx, nil, nil)
	setting := &settings.ExampleSettings{}
	err := m.Get(ctx, repository, "settingKey", setting)
	require.NoError(t, err)
	// should return empty string as checksum without an error
	checksum, err := m.GetLatest(ctx, repository, "settingKey", setting)
	require.NoError(t, err)
	require.NotNil(t, checksum)
	require.Equal(t, "", *checksum)
}

func TestSaveAndGet(t *testing.T) {
	ctx := context.Background()
	mc := &mockCache{
		c: make(map[interface{}]interface{}),
	}
	m := prepareTest(t, ctx, mc, nil)
	firstSettings := newSetting(5, 6, "hello")
	err := m.Save(ctx, repository, "settingKey", firstSettings, nil)
	testutil.Must(t, err)
	gotSettings := &settings.ExampleSettings{}
	err = m.Get(ctx, repository, "settingKey", gotSettings)
	testutil.Must(t, err)
	if diff := deep.Equal(firstSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	secondSettings := newSetting(15, 16, "hi")
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
	m := prepareTest(t, ctx, nil, nil)
	err := m.Save(ctx, repository, "settingKey", newSetting(5, 6, "hello"), nil)
	testutil.Must(t, err)
	setting := &settings.ExampleSettings{}
	eTag, err := m.GetLatest(ctx, repository, "settingKey", setting)
	testutil.Must(t, err)
	if diff := deep.Equal(newSetting(5, 6, "hello"), setting); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	if eTag == nil || *eTag == "" {
		t.Fatal("got empty eTag")
	}
}

func TestConditionalUpdate(t *testing.T) {
	ctx := context.Background()
	mc := &mockCache{
		c: make(map[interface{}]interface{}),
	}
	m := prepareTest(t, ctx, mc, nil)
	firstSettings := newSetting(5, 6, "hello")
	err := m.Save(ctx, repository, "settingKey", firstSettings, swag.String("WRONG_CHECKSUM"))
	require.ErrorIs(t, err, graveler.ErrPreconditionFailed)
	err = m.Save(ctx, repository, "settingKey", firstSettings, swag.String(""))
	testutil.Must(t, err)
	gotSettings := &settings.ExampleSettings{}
	checksum, err := m.GetLatest(ctx, repository, "settingKey", gotSettings)
	testutil.Must(t, err)
	if diff := deep.Equal(firstSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	secondSettings := newSetting(15, 16, "hi")
	err = m.Save(ctx, repository, "settingKey", secondSettings, checksum)
	testutil.Must(t, err)
	err = m.Save(ctx, repository, "settingKey", secondSettings, swag.String("WRONG_CHECKSUM"))
	require.ErrorIs(t, err, graveler.ErrPreconditionFailed)
}

func prepareTest(t *testing.T, ctx context.Context, refCache cache.Cache, branchLockCallback func(context.Context, *graveler.RepositoryRecord, graveler.BranchID, func() (interface{}, error)) (interface{}, error)) *settings.Manager {
	ctrl := gomock.NewController(t)
	refManager := mock.NewMockRefManager(ctrl)

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
	return m
}

func newSetting(a int32, b int32, c string) *settings.ExampleSettings {
	return &settings.ExampleSettings{
		ExampleInt: a,
		ExampleStr: c,
		ExampleMap: map[string]int32{"boo": b},
	}
}
