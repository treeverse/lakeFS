package auth_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

// storedEmail reads the persisted, base64-encoded user email straight from the
// KV store so tests can assert stored comm prefs independently of any reader
// method on the manager.
func storedEmail(ctx context.Context, t *testing.T, store kv.Store) string {
	t.Helper()
	v, err := store.Get(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(auth.EmailKeyName)))
	require.NoError(t, err)
	decoded, err := base64.StdEncoding.DecodeString(string(v.Value))
	require.NoError(t, err)
	return string(decoded)
}

// TestSetCommPrefsOnce verifies comm prefs can be set once and a repeat call is
// rejected without overriding the stored values.
func TestSetCommPrefsOnce(t *testing.T) {
	ctx := t.Context()
	store := kvtest.GetStore(ctx, t)
	mgr := auth.NewKVMetadataManager("test", "id", "mem", store)

	_, err := mgr.SetCommPrefsOnce(ctx, &auth.CommPrefs{UserEmail: "first@acme.co"})
	require.NoError(t, err)
	require.Equal(t, "first@acme.co", storedEmail(ctx, t, store))

	// repeat is rejected and must not override the stored values
	_, err = mgr.SetCommPrefsOnce(ctx, &auth.CommPrefs{UserEmail: "second@acme.co"})
	require.ErrorIs(t, err, auth.ErrCommPrefsAlreadySet)

	require.Equal(t, "first@acme.co", storedEmail(ctx, t, store), "rejected call must not override stored prefs")

	set, err := mgr.IsCommPrefsSet(ctx)
	require.NoError(t, err)
	require.True(t, set)
}

// TestSetCommPrefsOnceAfterSetupWithoutEmail covers the two-step flow: Setup
// without email writes comm_prefs_set=false, then SetCommPrefsOnce flips it once.
func TestSetCommPrefsOnceAfterSetupWithoutEmail(t *testing.T) {
	ctx := t.Context()
	mgr := auth.NewKVMetadataManager("test", "id", "mem", kvtest.GetStore(ctx, t))

	// Setup without comm prefs records comm_prefs_set=false.
	_, err := mgr.UpdateCommPrefs(ctx, nil)
	require.NoError(t, err)
	set, err := mgr.IsCommPrefsSet(ctx)
	require.NoError(t, err)
	require.False(t, set)

	// Providing them later succeeds once.
	_, err = mgr.SetCommPrefsOnce(ctx, &auth.CommPrefs{UserEmail: "later@acme.co"})
	require.NoError(t, err)
	set, err = mgr.IsCommPrefsSet(ctx)
	require.NoError(t, err)
	require.True(t, set)

	// and a repeat is rejected.
	_, err = mgr.SetCommPrefsOnce(ctx, &auth.CommPrefs{UserEmail: "again@acme.co"})
	require.ErrorIs(t, err, auth.ErrCommPrefsAlreadySet)
}

// TestSetCommPrefsOnceConcurrent verifies that under concurrent submissions
// exactly one caller commits and the rest get ErrCommPrefsAlreadySet.
func TestSetCommPrefsOnceConcurrent(t *testing.T) {
	ctx := t.Context()
	mgr := auth.NewKVMetadataManager("test", "id", "mem", kvtest.GetStore(ctx, t))

	const n = 8
	var wg sync.WaitGroup
	var success, conflict atomic.Int32
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := mgr.SetCommPrefsOnce(ctx, &auth.CommPrefs{UserEmail: fmt.Sprintf("u%d@acme.co", i)})
			switch {
			case err == nil:
				success.Add(1)
			case errors.Is(err, auth.ErrCommPrefsAlreadySet):
				conflict.Add(1)
			default:
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), success.Load(), "exactly one writer should win")
	require.Equal(t, int32(n-1), conflict.Load())
}

// TestUpdateCommPrefsNilClearsCache verifies the cache follows KV: a nil update
// (e.g. a Setup retry) un-sticks a previously cached true, preventing the
// divergence where the process reports "set" while KV says otherwise.
func TestUpdateCommPrefsNilClearsCache(t *testing.T) {
	ctx := t.Context()
	mgr := auth.NewKVMetadataManager("test", "id", "mem", kvtest.GetStore(ctx, t))

	_, err := mgr.UpdateCommPrefs(ctx, &auth.CommPrefs{UserEmail: "a@acme.co"})
	require.NoError(t, err)
	set, err := mgr.IsCommPrefsSet(ctx)
	require.NoError(t, err)
	require.True(t, set)

	_, err = mgr.UpdateCommPrefs(ctx, nil)
	require.NoError(t, err)
	set, err = mgr.IsCommPrefsSet(ctx)
	require.NoError(t, err)
	require.False(t, set, "nil update must clear the cached true state")
}
