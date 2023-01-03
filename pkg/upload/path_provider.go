package upload

import (
	"path"
	"sync"
	"time"

	"github.com/rs/xid"
)

// PathProvider captures the requirements from PathPartitionProvider implementation to return a new path
type PathProvider interface {
	NewPath() string
	ResolvePathTime(addressPath string) (time.Time, error)
}

// PathPartitionProvider provides path by request to upload data. The provided path is built from '<prefix>/<partition>/<unique id>'.
// The partition is updated every interval or when the number of IDs generated passes the specified size.
type PathPartitionProvider struct {
	mu  sync.Mutex
	cfg *PathProviderConfig
	// timestamp used to calculate the time based ID value
	timestamp time.Time
	// count make sure we will not produce more than 'count' IDs
	count int
	// partition current set and reset by size and time
	partition string
}

const (
	DefaultDataPartitionSize = 50000
	DefaultDataInterval      = time.Hour
	DefaultDataPrefix        = "data"

	// unixYear4000 epoch value for Saturday, January 1, 4000 12:00:00 AM. Changing this value is a breaking change as it is used to have reverse order for time based unique ID (xid).
	unixYear4000 = 64060588800
)

type TimeNow func() time.Time

var (
	DefaultPathProvider = NewPathPartitionProvider()
	DefaultTimeNow      = time.Now
)

type PathProviderConfig struct {
	Size     int
	Interval time.Duration
	Prefix   string
	TellTime TimeNow
}

type PathProviderOption func(o *PathProviderConfig)

func WithPathProviderSize(size int) PathProviderOption {
	return func(cfg *PathProviderConfig) {
		cfg.Size = size
	}
}

func WithPathProviderInterval(d time.Duration) PathProviderOption {
	return func(cfg *PathProviderConfig) {
		cfg.Interval = d
	}
}

func WithPathProviderPrefix(p string) PathProviderOption {
	return func(cfg *PathProviderConfig) {
		cfg.Prefix = p
	}
}

func WithPathProviderTellTime(t TimeNow) PathProviderOption {
	return func(cfg *PathProviderConfig) {
		cfg.TellTime = t
	}
}

func NewPathPartitionProvider(opts ...PathProviderOption) *PathPartitionProvider {
	cfg := &PathProviderConfig{
		Size:     DefaultDataPartitionSize,
		Interval: DefaultDataInterval,
		Prefix:   DefaultDataPrefix,
		TellTime: DefaultTimeNow,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	now := cfg.TellTime()
	return &PathPartitionProvider{
		cfg:       cfg,
		timestamp: now,
		count:     0,
		partition: newDescendingID(now).String(),
	}
}

// NewPath returns a new upload path based on the current partition.
func (i *PathPartitionProvider) NewPath() string {
	part := i.partitionForID()
	name := xid.New().String()
	return path.Join(i.cfg.Prefix, part, name)
}

func (i *PathPartitionProvider) ResolvePathTime(addressPath string) (time.Time, error) {
	_, address := path.Split(addressPath)
	id, err := xid.FromString(address)
	if err != nil {
		return time.Time{}, err
	}
	return id.Time(), nil
}

// partitionForID return the current partition to use. It will update the partition value before return in case we
// crossed the size or internal (time) configured.
func (i *PathPartitionProvider) partitionForID() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.count++
	// update partition value in case interval or size crossed the threshold
	if (i.count > i.cfg.Size) || (time.Since(i.timestamp) > i.cfg.Interval) {
		i.count = 1
		i.timestamp = i.cfg.TellTime()
		i.partition = newDescendingID(i.timestamp).String()
	}
	return i.partition
}

func newDescendingID(tm time.Time) xid.ID {
	t := time.Unix(unixYear4000-tm.Unix(), 0).UTC()
	return xid.NewWithTime(t)
}
