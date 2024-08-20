package esti

import (
	"strconv"
	"testing"

	"github.com/spf13/viper"
)

func LakefsWithParams(connectionString string) string {
	return LakefsWithParamsWithBasicAuth(connectionString, false)
}

func LakefsWithParamsWithBasicAuth(connectionString string, basicAuth bool) string {
	lakefsCmdline := "LAKEFS_DATABASE_TYPE=postgres" +
		" LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=" + connectionString +
		" LAKEFS_AUTH_INTERNAL_BASIC=" + strconv.FormatBool(basicAuth) +
		" LAKEFS_BLOCKSTORE_TYPE=" + viper.GetString("blockstore_type") +
		" LAKEFS_AUTH_ENCRYPT_SECRET_KEY='some random secret string' " + lakefsLocation()

	return lakefsCmdline
}

func lakefsLocation() string {
	return viper.GetString("binaries_dir") + "/lakefs"
}

func LakefsWithBasicAuth() string {
	return LakefsWithParamsWithBasicAuth(viper.GetString("database_connection_string"), true)
}

func Lakefs() string {
	return LakefsWithParams(viper.GetString("database_connection_string"))
}

func RequirePostgresDB(t *testing.T) {
	dbString := viper.GetString("database_connection_string")
	if dbString == "" {
		t.Skip("skip test - not postgres")
	}
}
