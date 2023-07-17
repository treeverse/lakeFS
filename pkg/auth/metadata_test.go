package auth_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func validateInstrumentation(t *testing.T, ctx context.Context, mgr *auth.KVMetadataManager, expected string) {
	md, err := mgr.GetMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, expected, md["instrumentation"])
}

func TestInstrumentation(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)

	_ = kvStore.Set(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(auth.SetupTimestampKeyName)), []byte(time.Now().Format(time.RFC3339)))
	mgr := auth.NewKVMetadataManager("test", "12345", "mem", kvStore)

	// No env vars - defaults to run
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationRun)

	// Add docker env file
	auth.StatFunc = func(name string) (os.FileInfo, error) {
		return nil, nil // return no error as if the file is found
	}
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationDocker)

	// Add quickstart env var with wrong value
	require.NoError(t, os.Setenv("QUICKSTART", "fff"))
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationDocker)

	// Add quickstart env var with right value
	require.NoError(t, os.Setenv("QUICKSTART", "true"))
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationQuickstart)

	// Add sample repo env var
	require.NoError(t, os.Setenv("LAKEFS_ACCESS_KEY_ID", "LKFSSAMPLES"))
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationSamplesRepo)

	// Add K8s env var
	require.NoError(t, os.Setenv("KUBERNETES_SERVICE_HOST", ""))
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationK8)
}
