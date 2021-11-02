package settings_test

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/testutil"
	"google.golang.org/protobuf/proto"
)

type mockCache struct {
	c map[interface{}]interface{}
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
	mockCache := &mockCache{
		c: make(map[interface{}]interface{}),
	}
	m, _ := prepareTest(t, ctx, mockCache, nil)
	emptySettings := &settings.ExampleSettings{}
	firstSettings := &settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}
	err := m.Save(ctx, "example-repo", "settingKey", firstSettings)
	testutil.Must(t, err)
	gotSettings, err := m.Get(ctx, "example-repo", "settingKey", emptySettings)
	testutil.Must(t, err)
	if diff := deep.Equal(firstSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	secondSettings := &settings.ExampleSettings{ExampleInt: 15, ExampleStr: "hi", ExampleMap: map[string]int32{"boo": 16}}
	err = m.Save(ctx, "example-repo", "settingKey", secondSettings)
	testutil.Must(t, err)
	// the result should be cached and we should get the first settings:
	gotSettings, err = m.Get(ctx, "example-repo", "settingKey", emptySettings)
	testutil.Must(t, err)
	if diff := deep.Equal(firstSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
	// after clearing the cache, we should get the new settings:
	mockCache.c = make(map[interface{}]interface{})
	gotSettings, err = m.Get(ctx, "example-repo", "settingKey", emptySettings)
	testutil.Must(t, err)
	if diff := deep.Equal(secondSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
}

func TestUpdateWithLock(t *testing.T) {
	ctx := context.Background()
	var lockStartWaitGroup sync.WaitGroup
	var lock sync.Mutex
	const IncrementCount = 20
	lockStartWaitGroup.Add(IncrementCount)

	m, _ := prepareTest(t, ctx, nil, func(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID, f func() (interface{}, error)) (interface{}, error) {
		lockStartWaitGroup.Done()
		lockStartWaitGroup.Wait() // wait until all goroutines ask for the lock
		lock.Lock()
		retVal, err := f()
		lock.Unlock()
		return retVal, err
	})
	emptySettings := &settings.ExampleSettings{}
	expectedSettings := &settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}
	err := m.Save(ctx, "example-repo", "settingKey", expectedSettings)
	testutil.Must(t, err)
	update := func(settingsToEdit proto.Message) error {
		settingsToEdit.(*settings.ExampleSettings).ExampleInt++
		settingsToEdit.(*settings.ExampleSettings).ExampleMap["boo"]++
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(IncrementCount)
	for i := 0; i < IncrementCount; i++ {
		go func() {
			testutil.Must(t, m.UpdateWithLock(ctx, "example-repo", "settingKey", emptySettings, update))
			wg.Done()
		}()
	}
	wg.Wait()
	testutil.Must(t, err)
	gotSettings, err := m.Get(ctx, "example-repo", "settingKey", emptySettings)
	testutil.Must(t, err)
	expectedSettings.ExampleInt += IncrementCount
	expectedSettings.ExampleMap["boo"] += IncrementCount
	if diff := deep.Equal(expectedSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
}

func TestStoredSettings(t *testing.T) {
	ctx := context.Background()
	m, blockAdapter := prepareTest(t, ctx, nil, nil)
	expectedSettings := &settings.ExampleSettings{ExampleInt: 5, ExampleStr: "hello", ExampleMap: map[string]int32{"boo": 6}}
	err := m.Save(ctx, "example-repo", "settingKey", expectedSettings)
	testutil.Must(t, err)
	reader, err := blockAdapter.Get(ctx, block.ObjectPointer{
		StorageNamespace: "mem://my-storage",
		Identifier:       "_lakefs/settings/settingKey.json",
		IdentifierType:   block.IdentifierTypeRelative,
	}, -1)
	testutil.Must(t, err)
	bytes, err := io.ReadAll(reader)
	testutil.Must(t, err)
	gotSettings := &settings.ExampleSettings{}
	testutil.Must(t, proto.Unmarshal(bytes, gotSettings))
	if diff := deep.Equal(expectedSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
}

// TestEmpty tests the setting store for keys which have not been set.
func TestEmpty(t *testing.T) {
	ctx := context.Background()
	m, _ := prepareTest(t, ctx, nil, nil)
	emptySettings := &settings.ExampleSettings{}
	gotSettings, err := m.Get(ctx, "example-repo", "settingKey", emptySettings)
	// the key was not set, an error should be returned
	if err != graveler.ErrNotFound {
		t.Fatalf("expected error %v, got %v", graveler.ErrNotFound, err)
	}
	// when using UpdateWithLock on an unset key, the update function gets an empty setting object to operate on
	err = m.UpdateWithLock(ctx, "example-repo", "settingKey", emptySettings, func(setting proto.Message) error {
		s := setting.(*settings.ExampleSettings)
		if s.ExampleMap == nil {
			s.ExampleMap = make(map[string]int32)
		}
		s.ExampleInt++
		s.ExampleMap["boo"]++
		return nil
	})
	testutil.Must(t, err)
	gotSettings, err = m.Get(ctx, "example-repo", "settingKey", emptySettings)
	testutil.Must(t, err)
	expectedSettings := &settings.ExampleSettings{ExampleInt: 1, ExampleMap: map[string]int32{"boo": 1}}
	if diff := deep.Equal(expectedSettings, gotSettings); diff != nil {
		t.Fatal("got unexpected settings:", diff)
	}
}

func prepareTest(t *testing.T, ctx context.Context, cache cache.Cache, branchLockCallback func(context.Context, graveler.RepositoryID, graveler.BranchID, func() (interface{}, error)) (interface{}, error)) (*settings.Manager, block.Adapter) {
	ctrl := gomock.NewController(t)
	refManager := mock.NewMockRefManager(ctrl)
	repo := &graveler.Repository{
		StorageNamespace: "mem://my-storage",
		DefaultBranchID:  "main",
	}
	blockAdapter := mem.New()
	branchLock := mock.NewMockBranchLocker(ctrl)
	cb := func(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID, f func() (interface{}, error)) (interface{}, error) {
		return f()
	}
	if branchLockCallback != nil {
		cb = branchLockCallback
	}
	var opts []settings.ManagerOption
	if cache != nil {
		opts = append(opts, settings.WithCache(cache))
	}
	branchLock.EXPECT().MetadataUpdater(ctx, gomock.Eq(graveler.RepositoryID("example-repo")), graveler.BranchID("main"), gomock.Any()).DoAndReturn(cb).AnyTimes()
	m := settings.NewManager(refManager, branchLock, blockAdapter, "_lakefs", opts...)
	refManager.EXPECT().GetRepository(ctx, gomock.Eq(graveler.RepositoryID("example-repo"))).AnyTimes().Return(repo, nil)
	return m, blockAdapter
}
