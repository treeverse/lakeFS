package esti

import (
	"testing"

	"github.com/spf13/viper"
)

func LakefsWithParams(connectionString string) string {
	return "LAKEFS_DATABASE_TYPE=postgres" +
		" LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING=" + connectionString +
		" LAKEFS_BLOCKSTORE_TYPE=" + viper.GetString("blockstore_type") +
		" LAKEFS_AUTH_ENCRYPT_SECRET_KEY='some random secret string' " + lakefsLocation()
}

func lakefsLocation() string {
	return lakeBinaryLocation("lakefs")
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
