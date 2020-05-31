package loadtesting

import (
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/testutil"
	"log"
	"os"
	"testing"
	"time"
)

var (
	pool        *dockertest.Pool
	databaseUri string
)

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	databaseUri, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

type mockCollector struct{}

func (m *mockCollector) Collect(_, _ string) {}

func TestLocalLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load tests in short mode")
	}
	mdb, mdbURI := testutil.GetDB(t, databaseUri, config.SchemaMetadata)
	blockAdapter := testutil.GetBlockAdapter(t, &block.NoOpTranslator{})

	meta := index.NewDBIndex(mdb)

	adb, adbURI := testutil.GetDB(t, databaseUri, config.SchemaAuth)
	authService := auth.NewDBAuthService(adb, crypt.NewSecretStore([]byte("some secret")))
	user := &authmodel.User{
		Email:    "admin@example.com",
		FullName: "admin user",
	}
	migrator := db.NewDatabaseMigrator().
		AddDB(config.SchemaMetadata, mdbURI).
		AddDB(config.SchemaAuth, adbURI)

	server := api.NewServer(
		meta,
		blockAdapter,
		authService,
		&mockCollector{},
		migrator,
	)
	listenAddress := "localhost:8981"
	go server.Listen(listenAddress)
	//time.Sleep(1 * time.Second)
	testutil.Must(t, authService.CreateUser(user))
	// create role
	role := &authmodel.Role{
		DisplayName: "Admins",
	}
	testutil.Must(t, authService.CreateRole(role))

	// attach policies
	policies := []*authmodel.Policy{
		{
			Permission: string(permissions.ManageRepos),
			Arn:        "arn:treeverse:repos:::*",
		},
		{
			Permission: string(permissions.ReadRepo),
			Arn:        "arn:treeverse:repos:::*",
		},
		{
			Permission: string(permissions.WriteRepo),
			Arn:        "arn:treeverse:repos:::*",
		},
	}
	for _, policy := range policies {
		testutil.Must(t, authService.AssignPolicyToRole(role.Id, policy))
	}

	// assign user to role
	testutil.Must(t, authService.AssignRoleToUser(role.Id, user.Id))
	credentials, err := authService.CreateUserCredentials(user)
	if err != nil {
		t.Fatalf("Failed to create user credentials: %v", err)
	}
	testerConfig := LoadTesterConfig{
		FreqPerSecond: 10,
		Duration:      15 * time.Second,
	}
	time.Sleep(1 * time.Second)
	err = loadTest(testerConfig, listenAddress, credentials)
	if err != nil {
		t.Fatalf("Got errors on test")
	}
}
