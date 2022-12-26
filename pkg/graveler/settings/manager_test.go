package settings_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/testutil"
	"google.golang.org/protobuf/proto"
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
	emptySettings := &settings.ExampleSettings{}
	firstSettings := &settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}
	err := m.Save(ctx, repository, "settingKey", firstSettings)
	testutil.Must(t, err)
	gotSettings, err := m.Get(ctx, repository, "settingKey", emptySettings)
	testutil.Must(t, err)
	if diff := deep.Equal(firstSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	secondSettings := &settings.ExampleSettings{ExampleInt: 15, ExampleStr: "hi", ExampleMap: map[string]int32{"boo": 16}}
	err = m.Save(ctx, repository, "settingKey", secondSettings)
	testutil.Must(t, err)
	// the result should be cached, and we should get the first settings:
	gotSettings, err = m.Get(ctx, repository, "settingKey", emptySettings)
	testutil.Must(t, err)
	if diff := deep.Equal(firstSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	// after clearing the mc, we should get the new settings:
	mc.c = make(map[interface{}]interface{})
	gotSettings, err = m.Get(ctx, repository, "settingKey", emptySettings)
	testutil.Must(t, err)
	if diff := deep.Equal(secondSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	m, _ := prepareTest(t, ctx, nil, nil)
	initialData := &settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}
	testutil.Must(t, m.Save(ctx, repository, "settingKey", initialData))

	validationData := &settings.ExampleSettings{
		ExampleInt: initialData.ExampleInt + 1,
		ExampleStr: "goodbye",
		ExampleMap: map[string]int32{"boo": initialData.ExampleMap["boo"] + 1},
	}
	update := func(settingsToEdit proto.Message) (proto.Message, error) {
		newSettings := settings.ExampleSettings{ExampleMap: map[string]int32{}}
		newSettings.ExampleStr = validationData.ExampleStr
		newSettings.ExampleInt = validationData.ExampleInt
		newSettings.ExampleMap["boo"] = validationData.ExampleMap["boo"]
		return &newSettings, nil
	}
	emptySettings := &settings.ExampleSettings{}
	require.NoError(t, m.Update(ctx, repository, "settingKey", emptySettings, update))
	gotSettings, err := m.Get(ctx, repository, "settingKey", emptySettings)
	require.NoError(t, err)
	if diff := deep.Equal(validationData, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}

	badData := &settings.ExampleSettings{
		ExampleInt: initialData.ExampleInt + 1,
		ExampleStr: "bad",
		ExampleMap: map[string]int32{"boo": -1},
	}

	// Failed update attempt with retry
	update = func(settingsToEdit proto.Message) (proto.Message, error) {
		newSettings := settings.ExampleSettings{ExampleMap: map[string]int32{}}

		newSettings.ExampleStr = initialData.ExampleStr
		newSettings.ExampleInt = initialData.ExampleInt
		newSettings.ExampleMap["boo"] = initialData.ExampleMap["boo"]
		require.NoError(t, m.Save(ctx, repository, "settingKey", badData))
		return &newSettings, nil
	}
	require.NoError(t, m.Update(ctx, repository, "settingKey", emptySettings, update))
	gotSettings, err = m.Get(ctx, repository, "settingKey", emptySettings)
	require.NoError(t, err)
	if diff := deep.Equal(initialData, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}

	// Failed update attempt with retry exhausted
	update = func(settingsToEdit proto.Message) (proto.Message, error) {
		newSettings := settings.ExampleSettings{ExampleMap: map[string]int32{}}

		validationData.ExampleInt++
		require.NoError(t, m.Save(ctx, repository, "settingKey", validationData))
		return &newSettings, nil
	}
	require.ErrorIs(t, m.Update(ctx, repository, "settingKey", emptySettings, update), graveler.ErrTooManyTries)
	gotSettings, err = m.GetLatest(ctx, repository, "settingKey", emptySettings)
	require.NoError(t, err)
	if diff := deep.Equal(validationData, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}

	// Failed update attempt with unknown error
	testErr := errors.New("test error")
	update = func(settingsToEdit proto.Message) (proto.Message, error) {
		return nil, testErr
	}
	require.ErrorIs(t, m.Update(ctx, repository, "settingKey", emptySettings, update), testErr)
	gotSettings, err = m.GetLatest(ctx, repository, "settingKey", emptySettings)
	require.NoError(t, err)
	if diff := deep.Equal(validationData, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
}

// TODO: locking is irrelevant for KV, create a new test for it
// Relevant only to DB implementation since KV is lockless
func TestMultipleUpdates(t *testing.T) {
	ctx := context.Background()
	const IncrementCount = 20
	var lockStartWaitGroup sync.WaitGroup
	var lock sync.Mutex
	lockStartWaitGroup.Add(IncrementCount)

	m, _ := prepareTest(t, ctx, nil, func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f func() (interface{}, error)) (interface{}, error) {
		lockStartWaitGroup.Done()
		lockStartWaitGroup.Wait() // wait until all goroutines ask for the lock
		lock.Lock()
		retVal, err := f()
		lock.Unlock()
		return retVal, err
	})
	emptySettings := &settings.ExampleSettings{}
	expectedSettings := &settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}
	err := m.Save(ctx, repository, "settingKey", expectedSettings)
	testutil.Must(t, err)
	update := func(settingsToEdit proto.Message) (proto.Message, error) {
		newSettings := settings.ExampleSettings{ExampleMap: map[string]int32{}}

		newSettings.ExampleStr = settingsToEdit.(*settings.ExampleSettings).ExampleStr
		newSettings.ExampleInt = settingsToEdit.(*settings.ExampleSettings).ExampleInt + 1
		newSettings.ExampleMap["boo"] = settingsToEdit.(*settings.ExampleSettings).ExampleMap["boo"] + 1
		return &newSettings, nil
	}
	var wg multierror.Group
	for i := 0; i < IncrementCount; i++ {
		wg.Go(func() error {
			return m.Update(ctx, repository, "settingKey", &settings.ExampleSettings{}, update)
		})
	}
	err = wg.Wait().ErrorOrNil()
	testutil.Must(t, err)
	gotSettings, err := m.Get(ctx, repository, "settingKey", emptySettings)
	testutil.Must(t, err)
	expectedSettings.ExampleInt += IncrementCount
	expectedSettings.ExampleMap["boo"] += IncrementCount
	if diff := deep.Equal(expectedSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
}

// TestEmpty tests the setting store for keys which have not been set.
func TestEmpty(t *testing.T) {
	ctx := context.Background()
	m, _ := prepareTest(t, ctx, nil, nil)
	emptySettings := &settings.ExampleSettings{}
	_, err := m.Get(ctx, repository, "settingKey", emptySettings)
	// the key was not set, an error should be returned
	if err != graveler.ErrNotFound {
		t.Fatalf("expected error %v, got %v", graveler.ErrNotFound, err)
	}
	// when using Update on an unset key, the update function gets an empty setting object to operate on
	err = m.Update(ctx, repository, "settingKey", emptySettings, func(setting proto.Message) (proto.Message, error) {
		newSettings := proto.Clone(setting).(*settings.ExampleSettings)

		if newSettings.ExampleMap == nil {
			newSettings.ExampleMap = make(map[string]int32)
		}
		newSettings.ExampleInt++
		newSettings.ExampleMap["boo"]++
		return newSettings, nil
	})
	testutil.Must(t, err)
	gotSettings, err := m.Get(ctx, repository, "settingKey", emptySettings)
	testutil.Must(t, err)
	expectedSettings := &settings.ExampleSettings{ExampleInt: 1, ExampleMap: map[string]int32{"boo": 1}}
	if diff := deep.Equal(expectedSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
}

func prepareTest(t *testing.T, ctx context.Context, refCache cache.Cache, branchLockCallback func(context.Context, *graveler.RepositoryRecord, graveler.BranchID, func() (interface{}, error)) (interface{}, error)) (*settings.Manager, block.Adapter) {
	ctrl := gomock.NewController(t)
	refManager := mock.NewMockRefManager(ctrl)

	blockAdapter := mem.New()
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
	m := settings.NewManager(refManager, kv.StoreMessage{Store: kvStore}, opts...)

	refManager.EXPECT().GetRepository(ctx, gomock.Eq(repository.RepositoryID)).AnyTimes().Return(repository, nil)
	return m, blockAdapter
}
