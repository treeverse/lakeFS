package params

import "time"

type ServiceCache struct {
	Enabled        bool
	Size           int
	TTL            time.Duration
	EvictionJitter time.Duration
}
