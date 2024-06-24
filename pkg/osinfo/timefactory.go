package osinfo

import "time"

const (
	MockNowDefault = 10000000000 // millis
)

// TimeFactory Interface for time functions we may want to mock in testing
type TimeFactory interface {
	Now() time.Time
}

// SystemTimeFactory default factory for getting system type
type SystemTimeFactory struct {
}

func (m *SystemTimeFactory) Now() time.Time {
	return time.Now()
}

// MockTimeFactory mock implementation
type MockTimeFactory struct {
	NowTime time.Time
}

func (m *MockTimeFactory) Now() time.Time {
	return m.NowTime
}
