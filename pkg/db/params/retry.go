package params

import (
	"time"

	"gopkg.in/retry.v1"
)

const (
	databaseFirstWait  = 50 * time.Millisecond
	databaseWaitGrowth = 1.2
	databaseMaxWait    = 3 * time.Second
)

var DatabaseRetryStrategy = retry.LimitTime(databaseMaxWait,
	retry.Exponential{
		Initial: databaseFirstWait,
		Factor:  databaseWaitGrowth,
		Jitter:  true,
	},
)
