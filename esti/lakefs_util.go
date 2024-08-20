package esti

import (
	"strconv"
	"testing"

	"github.com/spf13/viper"
)

func LakefsWithParams(connectionString string) string {
	return LakefsWithParamsWithBasicAuth(connectionString, false)
}

func LakefsWithParamsWithBasicAuth(connectionString string, BasicAuth bool) string {
	lakefsCmdline := "LAKEFS_DATABASE_TYPE=postgres" +
		" LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=" + connectionString +
		" LAKEFS_AUTH_INTERNAL_BASIC=" + strconv.FormatBool(BasicAuth) +
		" " + lakefsLocation()

	return lakefsCmdline
}

func lakefsLocation() string {
	return viper.GetString("binaries_dir") + "/lakefs"
}

func LakefsWithBasicAuth(t *testing.T) string {
	dbString := viper.GetString("database_connection_string")
	if dbString == "" {
		t.Skip("database connection string not set")
	}
	return LakefsWithParamsWithBasicAuth(dbString, true)
}

func Lakefs() string {
	return LakefsWithParams(viper.GetString("database_connection_string"))
}
