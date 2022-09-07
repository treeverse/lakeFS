package auth_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	MaxCredsPerUser = 4
	NumUsers        = 20
	NumGroups       = 4
	HugePageSize    = 1000
)

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	database, _ := testutil.GetDB(t, databaseURI)
	kvStore := kvtest.MakeStoreByName(postgres.DriverName, kvparams.KV{Postgres: &kvparams.Postgres{ConnectionString: databaseURI}})(t, ctx)
	defer kvStore.Close()
	dbAuthService := auth.NewDBAuthService(database, crypt.NewSecretStore([]byte("someSecret")), nil, authparams.ServiceCache{
		Enabled: false,
	}, logging.Default())
	dbMetadataMgr := auth.NewDBMetadataManager("ver_kv_migrate_test", "id-kv-migrate-test", database)

	prepareTestData(t, ctx, dbAuthService)
	vals, isInit := generateMetadata(t, ctx, dbMetadataMgr)

	buf := bytes.Buffer{}
	require.NoError(t, auth.Migrate(ctx, database.Pool(), nil, &buf))

	testutil.MustDo(t, "Import migrated auth data", kv.Import(ctx, &buf, kvStore))
	kvAuthService := auth.NewKVAuthService(&kv.StoreMessage{Store: kvStore}, crypt.NewSecretStore([]byte("someSecret")), nil, authparams.ServiceCache{
		Enabled: false,
	}, logging.Default())
	kvMetadataManager := auth.NewKVMetadataManager("ver_kv_migrate_test", "id-kv-migrate-test", "kv-type", kvStore)
	verifyMigrationResults(t, ctx, dbAuthService, kvAuthService)
	verifyMetadata(t, ctx, kvMetadataManager, kvStore, vals, getExcludeVals(t, ctx, database), isInit)
}

func prepareTestData(t *testing.T, ctx context.Context, svc auth.Service) {
	userNames := generateUserNames(NumUsers)
	for _, userName := range userNames {
		createTestUserWithCreds(t, ctx, svc, userName, rand.Intn(MaxCredsPerUser+1))
	}

	groupNames := []string{"allUsersGroup", "noUsersGroup", "someUsersGroup", "someOtherUsersGroup"}
	createGroupWithUsers(t, ctx, svc, groupNames[0], userNames)
	createGroupWithUsers(t, ctx, svc, groupNames[1], []string{})
	createGroupWithUsers(t, ctx, svc, groupNames[2], userNames[3:8])
	createGroupWithUsers(t, ctx, svc, groupNames[3], userNames[6:16])

	policies := writePolicies(t, ctx, svc, NumUsers/2+1)
	for i := 1; i <= NumUsers/2; i++ {
		for j := 0; j < NumUsers; j += i {
			if err := svc.AttachPolicyToUser(ctx, policies[i], userNames[j]); err != nil {
				t.Fatalf("AttachPolicyToUser(%s, %s): %s", policies[i], userNames[j], err)
			}
		}
	}

	for i := 0; i < NumGroups; i++ {
		for j := 0; j <= i; j++ {
			if err := svc.AttachPolicyToGroup(ctx, policies[i], groupNames[j]); err != nil {
				t.Fatalf("AttachPolicyToGroup(%s, %s): %s", policies[i], groupNames[j], err)
			}
		}
	}
}

func generateUniqueName(suffix string) string {
	return fmt.Sprintf("%s_%s", model.CreateID(), suffix)
}

func generateUserNames(num int) []string {
	var names []string
	for i := 0; i < num; i++ {
		names = append(names, generateUniqueName("user"))
	}
	return names
}

func generateRandomArn() string {
	return fmt.Sprintf("arn:lakefs:auth:::%s", model.CreateID())
}

func createTestUserWithCreds(t *testing.T, ctx context.Context, svc auth.Service, userName string, numCred int) {
	if _, err := svc.CreateUser(ctx, &model.User{Username: userName}); err != nil {
		t.Fatalf("CreateUser(%s): %s", userName, err)
	}
	for i := 0; i < numCred; i++ {
		_, err := svc.CreateCredentials(ctx, userName)
		if err != nil {
			t.Fatalf("CreateCredentials(%s): %s", userName, err)
		}
	}
}

func createGroupWithUsers(t *testing.T, ctx context.Context, svc auth.Service, groupName string, users []string) {
	if err := svc.CreateGroup(ctx, &model.Group{DisplayName: groupName}); err != nil {
		t.Fatalf("CreateGroup(%s): %s", groupName, err)
	}
	for _, userName := range users {
		if err := svc.AddUserToGroup(ctx, userName, groupName); err != nil {
			t.Fatalf("AddUserToGroup(%s,%s): %s", userName, groupName, err)
		}
	}
}

func writePolicies(t *testing.T, ctx context.Context, svc auth.Service, num int) []string {
	var names []string
	for i := 0; i < num; i++ {
		names = append(names, writePolicy(t, ctx, svc))
	}
	return names
}

func writePolicy(t *testing.T, ctx context.Context, svc auth.Service) string {
	policyName := generateUniqueName("policy")
	if err := svc.WritePolicy(ctx, &model.Policy{
		DisplayName: policyName,
		Statement: model.Statements{
			{
				Effect:   "allow",
				Action:   []string{"fs:WriteObject"},
				Resource: generateRandomArn(),
			}, {
				Effect:   "deny",
				Action:   []string{"fs:WriteObject"},
				Resource: generateRandomArn(),
			},
		},
	}); err != nil {
		t.Fatalf("WritePolicy(%s): %s", policyName, err)
	}
	return policyName
}

func verifyMigrationResults(t *testing.T, ctx context.Context, srcSvc, dstSvc auth.Service) {
	// verifying bidirectional inclusion
	verifyInclusion(t, ctx, srcSvc, dstSvc)
	verifyInclusion(t, ctx, dstSvc, srcSvc)
}

// verifyInclusion - verifies that otherSvc contains all entities and relations from svc
func verifyInclusion(t *testing.T, ctx context.Context, svc, otherSvc auth.Service) {
	verifyUsersInclusion(t, ctx, svc, otherSvc)
	verifyGroupsInclusion(t, ctx, svc, otherSvc)
	verifyPoliciesInclusion(t, ctx, svc, otherSvc)
}

// verifyUsersInclusion - verifies all users and their correlated entities from svc exist in otherSvc
func verifyUsersInclusion(t *testing.T, ctx context.Context, svc, otherSvc auth.Service) {
	users, _, err := svc.ListUsers(ctx, &model.PaginationParams{Amount: HugePageSize})
	require.NoError(t, err)
	for _, user := range users {
		_, err := otherSvc.GetUser(ctx, user.Username)
		require.NoError(t, err)

		groups, _, err := svc.ListUserGroups(ctx, user.Username, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		otherGroups, _, err := otherSvc.ListUserGroups(ctx, user.Username, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		require.Equal(t, len(groups), len(otherGroups), "User groups length")
		groupsSet := make(map[string]bool)
		for _, group := range otherGroups {
			groupsSet[group.DisplayName] = true
		}
		for _, group := range groups {
			require.True(t, groupsSet[group.DisplayName], "Expected group for user not found", group.DisplayName)
		}

		policies, _, err := svc.ListUserPolicies(ctx, user.Username, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		otherPolicies, _, err := otherSvc.ListUserPolicies(ctx, user.Username, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		require.Equal(t, len(policies), len(otherPolicies), "User policies length")
		policiesSet := make(map[string]bool)
		for _, policy := range otherPolicies {
			policiesSet[policy.DisplayName] = true
		}
		for _, policy := range policies {
			require.True(t, policiesSet[policy.DisplayName], "Expected policy for user not found", policy.DisplayName)
		}

		effectivePolicies, _, err := svc.ListEffectivePolicies(ctx, user.Username, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		otherEffectivePolicies, _, err := otherSvc.ListEffectivePolicies(ctx, user.Username, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		require.Equal(t, len(effectivePolicies), len(otherEffectivePolicies), "User effective policies length")
		effectivePoliciesSet := make(map[string]bool)
		for _, policy := range otherEffectivePolicies {
			effectivePoliciesSet[policy.DisplayName] = true
		}
		for _, policy := range effectivePolicies {
			require.True(t, effectivePoliciesSet[policy.DisplayName], "Expected effective policy for user not found", policy.DisplayName)
		}

		creds, _, err := svc.ListUserCredentials(ctx, user.Username, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		otherCreds, _, err := otherSvc.ListUserCredentials(ctx, user.Username, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		require.Equal(t, len(creds), len(otherCreds), "User credentials length")
		credsMap := make(map[string][]byte)
		for _, cred := range otherCreds {
			credsMap[cred.AccessKeyID] = cred.SecretAccessKeyEncryptedBytes
		}
		for _, cred := range creds {
			encryptedSecretKey, ok := credsMap[cred.AccessKeyID]
			require.True(t, ok, "Expected credentials missing")
			require.Equal(t, encryptedSecretKey, cred.SecretAccessKeyEncryptedBytes)
		}
	}
}

// verifyGroupsInclusion - verifies all groups and their correlated entities from svc exist in otherSvc
func verifyGroupsInclusion(t *testing.T, ctx context.Context, svc, otherSvc auth.Service) {
	groups, _, err := svc.ListGroups(ctx, &model.PaginationParams{Amount: HugePageSize})
	require.NoError(t, err)
	for _, group := range groups {
		_, err := otherSvc.GetGroup(ctx, group.DisplayName)
		require.NoError(t, err)

		users, _, err := svc.ListGroupUsers(ctx, group.DisplayName, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		otherUsers, _, err := otherSvc.ListGroupUsers(ctx, group.DisplayName, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		require.Equal(t, len(users), len(otherUsers), "Group users length")
		usersSet := make(map[string]bool)
		for _, user := range otherUsers {
			usersSet[user.Username] = true
		}
		for _, user := range users {
			require.True(t, usersSet[user.Username], "Expected user for group not found", group.DisplayName)
		}

		policies, _, err := svc.ListGroupPolicies(ctx, group.DisplayName, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		otherPolicies, _, err := otherSvc.ListGroupPolicies(ctx, group.DisplayName, &model.PaginationParams{Amount: HugePageSize})
		require.NoError(t, err)
		require.Equal(t, len(policies), len(otherPolicies), "User policies length")
		policiesSet := make(map[string]bool)
		for _, policy := range otherPolicies {
			policiesSet[policy.DisplayName] = true
		}
		for _, policy := range policies {
			require.True(t, policiesSet[policy.DisplayName], "Expected policy for group not found", policy.DisplayName)
		}
	}
}

// verifyPoliciesInclusion - verifies all policies from svc exist in otherSvc and are identical
func verifyPoliciesInclusion(t *testing.T, ctx context.Context, svc, otherSvc auth.Service) {
	policies, _, err := svc.ListPolicies(ctx, &model.PaginationParams{Amount: HugePageSize})
	require.NoError(t, err)
	for _, policy := range policies {
		otherPolicy, err := otherSvc.GetPolicy(ctx, policy.DisplayName)
		require.NoError(t, err)
		require.Equal(t, policy.Statement, otherPolicy.Statement)
	}
}

func generateMetadata(t *testing.T, ctx context.Context, mgr *auth.DBMetadataManager) (map[string]string, bool) {
	metadata, err := mgr.Write(ctx)
	require.NoError(t, err)
	isInit, err := mgr.IsInitialized(ctx)
	require.NoError(t, err)
	return metadata, isInit
}

func verifyMetadata(t *testing.T, ctx context.Context, mgr *auth.KVMetadataManager, store kv.Store, valsToVerify, valsToExclude map[string]string, isInit bool) {
	kvIsInit, err := mgr.IsInitialized(ctx)
	require.NoError(t, err)
	require.Equal(t, isInit, kvIsInit)
	for k, v := range valsToVerify {
		if _, ok := valsToExclude[k]; ok {
			continue
		}
		kvKey := model.MetadataKeyPath(k)
		valWithPred, err := store.Get(ctx, []byte(model.PartitionKey), []byte(kvKey))
		require.NoError(t, err, "store.Get(%s, %s)", model.PartitionKey, kvKey)
		require.Equal(t, v, string(valWithPred.Value))
	}
}

func getExcludeVals(t *testing.T, ctx context.Context, db db.Database) map[string]string {
	dbMeta, err := db.Metadata(ctx)
	require.NoError(t, err)
	return dbMeta
}
