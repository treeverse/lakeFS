package params

import "time"

type Database struct {
	Driver                string
	ConnectionString      string
	MaxOpenConnections    int32
	MaxIdleConnections    int32
	ConnectionMaxLifetime time.Duration
	DisableAutoMigrate    bool
}
