package params

import "time"

type Database struct {
	Driver                string
	URI                   string
	MaxOpenConnections    int
	MaxIdleConnections    int
	ConnectionMaxLifetime time.Duration
}
