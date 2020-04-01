package merge

import (
	"github.com/treeverse/lakefs/api/gen/client"
	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/index/errors"
	"strconv"
	"testing"
)

var cs csvStore

func TestMerge(t *testing.T) {

	cs = make(csvStore)
	cs.addType("branches", []string{"name", "commit", "commitRoot", "workspaceRoot"})
	cs.addType("entries", []string{"owner", "name", "address", "type", "size", "checksum"})
	cs.addType("commits", []string{"address", "tree"})
	metadata := make(map[string]string)
	metadata["property-1"] = "value-1"

	t.Run("simplest merge", func(t *testing.T) {
		handler, deps, close := getHandler(t)
		defer close()
		creds, clt, _ := setupHelper(t, deps, handler)
		uploadObject(t, "t/v/s", "master", 1024, clt, creds)
		commit(t, "master", "master-1", clt, creds)
		createBranch(t, "br-1", "master", clt, creds)
		commit(t, "br-1", "br-1-1", clt, creds)

		uploadObject(t, "t/v1/s", "master", 10000, clt, creds)
		commit(t, "master", "master-2", clt, creds)
		_, _ = deps.meta.Merge("myrepo", "master", "br-1", "tzahiJ", "some message", metadata)
		_ = getObject(t, deps.meta, "myrepo", "br-1", "t/v1/s", true, "merge failed - document not copied")

		//showEntries(db.Store(deps.db), chacksumTranslat, cs, "1-")

	})

	t.Run("merge with removee", func(t *testing.T) {
		handler, deps, close := getHandler(t)
		defer close()
		creds, clt, _ := setupHelper(t, deps, handler)
		uploadObject(t, "t/v/s", "master", 1024, clt, creds)
		uploadObject(t, "t/v/s1", "master", 2048, clt, creds)
		commit(t, "master", "master-1", clt, creds)
		createBranch(t, "br-1", "master", clt, creds)
		commit(t, "br-1", "br-1-1", clt, creds)
		err := deps.meta.DeleteObject("myrepo", "master", "t/v/s")
		if err != nil {
			t.Error("could not delete object\n")
		}
		commit(t, "master", "master-2", clt, creds)
		_, _ = deps.meta.Merge("myrepo", "master", "br-1", "tzahiJ", "some message", metadata)
		_ = getObject(t, deps.meta, "myrepo", "br-1", "t/v/s", false, "merge failed - document not deleted")
		//showEntries(db.Store(deps.db), chacksumTranslat, cs, "2-")

	})

	t.Run("merge without last commit", func(t *testing.T) {
		handler, deps, close := getHandler(t)
		defer close()
		creds, clt, _ := setupHelper(t, deps, handler)
		uploadObject(t, "t/v/s", "master", 1024, clt, creds)
		commit(t, "master", "master-1", clt, creds)
		createBranch(t, "br-1", "master", clt, creds)
		commit(t, "br-1", "br-1-1", clt, creds)

		uploadObject(t, "t/v/s1", "master", 10000, clt, creds)

		_, _ = deps.meta.Merge("myrepo", "master", "br-1", "tzahiJ", "some message", metadata)
		_ = getObject(t, deps.meta, "myrepo", "br-1", "t/v/s1", false, "merge failed - uncommitted document synchronizes")

		//showEntries(db.Store(deps.db), chacksumTranslat, cs, "3-")
	})
	t.Run("merge with conflict", func(t *testing.T) {
		handler, deps, close := getHandler(t)
		defer close()
		creds, clt, _ := setupHelper(t, deps, handler)
		uploadObject(t, "t/v/s", "master", 1024, clt, creds)
		commit(t, "master", "master-1", clt, creds)
		createBranch(t, "br-1", "master", clt, creds)
		commit(t, "br-1", "br-1-1", clt, creds)

		uploadObject(t, "t/v/s1", "master", 10000, clt, creds)
		uploadObject(t, "t/v/s1", "br-1", 5000, clt, creds)
		commit(t, "br-1", "br-1-1", clt, creds)
		commit(t, "master", "master-2", clt, creds)

		_, err := deps.meta.Merge("myrepo", "master", "br-1", "tzahiJ", "with conflicte", metadata)
		if err != errors.ErrMergeConflict {
			t.Error("did not identify conflict  ", err)
		}
		//showEntries(db.Store(deps.db), chacksumTranslat, cs, "4-")

	})
	t.Run("large tree", func(t *testing.T) {
		handler, deps, close := getHandler(t)
		defer close()
		creds, clt, _ := setupHelper(t, deps, handler)
		uploadTree(t, "master", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 4096, clt, creds)
		commit(t, "master", "master-1", clt, creds)
		createBranch(t, "br-1", "master", clt, creds)
		commit(t, "br-1", "br-1-1", clt, creds)
		uploadTree(t, "br-1", "base", []string{"lva", "lvb"}, []int{0, 0}, []int{10, 10}, 5020, clt, creds)
		uploadTree(t, "master", "base", []string{"lv1", "lv2"}, []int{10, 0}, []int{10, 10}, 5020, clt, creds)
		commit(t, "br-1", "br-1-2", clt, creds)
		_, err := deps.meta.Merge("myrepo", "master", "br-1", "tzahiJ", "with conflicte", metadata)
		if err != nil {
			t.Error("failed large merge  ", err)
		}
		// showEntries(db.Store(deps.db), chacksumTranslat, cs, "5-")
	})

}

func uploadTree(t *testing.T, branch, base string, nm []string, startLevel, numInLevel []int, size int64, clt *client.Lakefs, creds *authmodel.APICredentials) {
	for i := 0; i < numInLevel[0]; i++ {
		for j := 0; j < numInLevel[1]; j++ {
			path := base + "/" + nm[0] + strconv.Itoa(i+startLevel[0]) + "/"
			path += nm[1] + strconv.Itoa(j+startLevel[1])
			uploadObject(t, path, branch, size, clt, creds)
		}
	}

	//	DeleteObject(repoId, branch, path string) error

}
