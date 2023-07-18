package auth_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func validateInstrumentation(t *testing.T, ctx context.Context, mgr *auth.KVMetadataManager, inst string, k8s, docker bool) {
	md, err := mgr.GetMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, inst, md["instrumentation"])
	require.Equal(t, strconv.FormatBool(k8s), md["is_k8s"])
	require.Equal(t, strconv.FormatBool(docker), md["is_docker"])
}

func TestInstrumentation(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)

	_ = kvStore.Set(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(auth.SetupTimestampKeyName)), []byte(time.Now().Format(time.RFC3339)))
	mgr := auth.NewKVMetadataManager("test", "12345", "mem", kvStore)

	// No env vars - defaults to run
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationRun, false, false)

	// Add quickstart env var with wrong value
	require.NoError(t, os.Setenv("QUICKSTART", "fff"))
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationRun, false, false)

	// Add docker env file
	auth.DockeEnvExists = ""
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationRun, false, true)

	// Add sample repo env var
	viper.Set("installation.access_key_id", "LKFSSAMPLES")
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationSamplesRepo, false, true)

	// Add quickstart env var with right value
	viper.Set("installation.access_key_id", "QUICKSTART")
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationQuickstart, false, true)

	// Add K8s env var
	require.NoError(t, os.Setenv("KUBERNETES_SERVICE_HOST", ""))
	validateInstrumentation(t, ctx, mgr, auth.InstrumentationQuickstart, true, true)
}
