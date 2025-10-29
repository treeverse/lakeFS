package auth_test

import (
	"context"
	"os"
	"runtime"
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
	viper.Set("installation.access_key_id", "SOMEKEY")
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

func TestMetadataFields(t *testing.T) {
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)

	const (
		installationID = "fa3920ce-5c58-417f-b672-7e0b2870fea9"
		version        = "test"
		kvType         = "mem"
	)

	// Create a new metadata manager with a fixed installation ID
	mgr := auth.NewKVMetadataManager(version, installationID, kvType, kvStore)

	// Get metadata and verify all fields
	md, err := mgr.GetMetadata(ctx)
	require.NoError(t, err)

	// Verify basic fields
	require.Equal(t, version, md["lakefs_version"])
	require.Equal(t, kvType, md["lakefs_kv_type"])
	require.Equal(t, installationID, md["installation_id"])
	require.Equal(t, runtime.Version(), md["golang_version"])
	require.Equal(t, runtime.GOARCH, md["architecture"])
	require.Equal(t, runtime.GOOS, md["os"])

	// Verify environment detection fields - Check if the keys exist
	require.Contains(t, md, "is_k8s")          // Check K8s property exists
	require.Contains(t, md, "is_docker")       // Check docker property exists
	require.Contains(t, md, "instrumentation") // Check docker property exists

	// Verify setup timestamp is not set initially
	require.Empty(t, md[auth.SetupTimestampKeyName])
}
