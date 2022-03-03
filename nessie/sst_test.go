package nessie

import (
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func TestCommitSSTHeaders(t *testing.T) {
	_, _, repo := setupTest(t)
	dumpPath := viper.GetString("storage_namespace") + "/" + repo + "/_lakefs/"

	refDumpsResult, err := runShellCommand(Lakectl()+" refs-dump lakefs://"+repo, false)
	require.NoError(t, err, "Failed to run shell command")
	commitsMetaRangeId := gjson.Get(string(refDumpsResult), "commits_meta_range_id")

	catSSTResult, err := runShellCommand("aws s3 cp "+dumpPath+commitsMetaRangeId.String()+" - | "+Lakectl()+" cat-sst -f -", false)
	require.NoError(t, err, "Failed to run shell command")

	// getting all files in dump dir
	s3LSOutput, err := runShellCommand("aws s3 ls "+dumpPath+" | awk '{print $4}'", false)
	require.NoError(t, err, "Failed to run shell command")

	filesInDumpDir := strings.Split(string(s3LSOutput), "\n")
	var commitsRange string
	for i := 0; i < len(filesInDumpDir); i++ {
		if strings.Contains(string(catSSTResult), filesInDumpDir[i]) {
			commitsRange = filesInDumpDir[i]
			break
		}
	}

	catSSTResul2, err := runShellCommand("aws s3 cp "+dumpPath+commitsRange+" - | "+Lakectl()+" cat-sst -f -", false)
	require.NoError(t, err, "Failed to run shell command")

	commitReflection := reflect.Indirect(reflect.ValueOf(graveler.Commit{}))
	for i := 0; i < commitReflection.NumField(); i++ {
		fieldName := strings.ToUpper(commitReflection.Type().Field(i).Name)
		require.True(t, strings.Contains(string(catSSTResul2), fieldName), "missing field: "+fieldName+" in cat-sst output: "+string(catSSTResul2))
	}

}
