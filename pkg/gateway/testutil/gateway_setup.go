package testutil

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/gateway"
	"github.com/treeverse/lakefs/pkg/gateway/multipart"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
)

type Dependencies struct {
	blocks  block.Adapter
	auth    *FakeAuthService
	catalog *catalog.Catalog
}

func GetBasicHandler(t *testing.T, authService *FakeAuthService, repoName string) (http.Handler, *Dependencies) {
	ctx := context.Background()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)

	store := kvtest.MakeStoreByName("mem", kvparams.Config{})(t, context.Background())
	defer store.Close()
	storeMessage := &kv.StoreMessage{Store: store}
	multipartTracker := multipart.NewTracker(*storeMessage)

	blockstoreType, _ := os.LookupEnv(testutil.EnvKeyUseBlockAdapter)
	blockAdapter := testutil.NewBlockAdapterByType(t, blockstoreType)

	conf, err := config.NewConfig()
	testutil.MustDo(t, "config", err)

	c, err := catalog.New(ctx, catalog.Config{
		Config:  conf,
		KVStore: storeMessage,
	})
	testutil.MustDo(t, "build catalog", err)
	t.Cleanup(func() {
		_ = c.Close()
	})

	storageNamespace := os.Getenv("USE_STORAGE_NAMESPACE")
	if storageNamespace == "" {
		storageNamespace = "replay"
	}

	_, err = c.CreateRepository(ctx, repoName, storageNamespace, "main")
	testutil.Must(t, err)

	handler := gateway.NewHandler(
		authService.Region,
		c,
		multipartTracker,
		blockAdapter,
		authService,
		[]string{authService.BareDomain},
		&stats.NullCollector{},
		upload.DefaultPathProvider,
		nil,
		config.DefaultAuditLogLevel,
		true,
	)

	return handler, &Dependencies{
		blocks:  blockAdapter,
		auth:    authService,
		catalog: c,
	}
}

type FakeAuthService struct {
	BareDomain      string `json:"bare_domain"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"access_secret_key"`
	UserID          string `json:"user_id"`
	Region          string `json:"region"`
}

func (m *FakeAuthService) GetCredentials(_ context.Context, accessKey string) (*model.Credential, error) {
	if accessKey != m.AccessKeyID {
		logging.Default().Fatal("access key in recording different than configuration")
	}
	aCred := new(model.Credential)
	aCred.AccessKeyID = accessKey
	aCred.SecretAccessKey = m.SecretAccessKey
	aCred.Username = m.UserID
	return aCred, nil
}

func (m *FakeAuthService) GetUser(_ context.Context, _ string) (*model.User, error) {
	return &model.User{
		CreatedAt: time.Now(),
		Username:  "user",
	}, nil
}

func (m *FakeAuthService) Authorize(_ context.Context, _ *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	return &auth.AuthorizationResponse{Allowed: true}, nil
}
