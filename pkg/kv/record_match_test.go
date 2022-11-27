package kv_test

import (
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestFindMessageTypeRecord(t *testing.T) {
	tests := []struct {
		name      string
		partition string
		path      string
		want      protoreflect.MessageType
	}{
		{name: "RepositoryData", partition: "graveler", path: "repos/test-repo", want: (&graveler.RepositoryData{}).ProtoReflect().Type()},
		{name: "BranchData", partition: "test", path: "branches/test-branch", want: (&graveler.BranchData{}).ProtoReflect().Type()},
		{name: "CommitData", partition: "test", path: "commits/1234556", want: (&graveler.CommitData{}).ProtoReflect().Type()},
		{name: "TagData", partition: "test", path: "tags/test-tag", want: (&graveler.TagData{}).ProtoReflect().Type()},
		{name: "StagedEntryData", partition: "test", path: "test", want: (&graveler.StagedEntryData{}).ProtoReflect().Type()},
		{name: "TaskResultData", partition: "test", path: "repos/test-repo/tasks/test-key", want: (&actions.TaskResultData{}).ProtoReflect().Type()},
		{name: "RunResultData", partition: "test", path: "repos/test-repo/runs/test-key", want: (&actions.RunResultData{}).ProtoReflect().Type()},
		{name: "branches_SecondaryIndex", partition: "test", path: "repos/test-repo/branches/test-branch/test-key", want: (&kv.SecondaryIndex{}).ProtoReflect().Type()},
		{name: "commits_SecondaryIndex", partition: "test", path: "repos/test-repo/commits/test-commit/test-key", want: (&kv.SecondaryIndex{}).ProtoReflect().Type()},
		{name: "UserData", partition: "auth", path: "users/test-user", want: (&model.UserData{}).ProtoReflect().Type()},
		{name: "PolicyData", partition: "auth", path: "policies/test-policy", want: (&model.PolicyData{}).ProtoReflect().Type()},
		{name: "GroupData", partition: "auth", path: "groups/test-group", want: (&model.GroupData{}).ProtoReflect().Type()},
		{name: "CredentialData", partition: "auth", path: "uCredentials/test-cred/credentials/test-key", want: (&model.CredentialData{}).ProtoReflect().Type()},
		{name: "users_SecondaryIndex", partition: "auth", path: "gUsers/test-user/users/test-key", want: (&kv.SecondaryIndex{}).ProtoReflect().Type()},
		{name: "group_policy_SecondaryIndex", partition: "auth", path: "gPolicies/test-group/policies/test-key", want: (&kv.SecondaryIndex{}).ProtoReflect().Type()},
		{name: "user_policy_SecondaryIndex", partition: "auth", path: "uPolicies/test-user-policy/policies/test-key", want: (&kv.SecondaryIndex{}).ProtoReflect().Type()},
		{name: "TokenData", partition: "auth", path: "expiredTokens/token12345", want: (&model.TokenData{}).ProtoReflect().Type()},
		{name: "installation_metadata", partition: "auth", path: "installation_metadata/installation_id", want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := kv.FindMessageTypeRecord(tt.partition, tt.path)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FindMessageTypeRecord() got = %v, want %v", got, tt.want)
			}
		})
	}
}
