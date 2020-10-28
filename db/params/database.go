package params

import "time"

type Database struct {
	Driver                string
	ConnectionString      string
	MaxOpenConnections    int
	MaxIdleConnections    int
	ConnectionMaxLifetime time.Duration
}
