package kv_test

import (
	"bytes"
	"context"
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
)

func TestImportEmptyFile(t *testing.T) {
	ctx := context.Background()
	store := kvtest.MakeStoreByName("mem", "")(t, context.Background())
	defer store.Close()

	buf := bytes.Buffer{}
	err := kv.Import(ctx, &buf, store)
	require.ErrorIs(t, err, kv.ErrInvalidFormat)
}
