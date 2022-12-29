package stats

type NullCollector struct{}

func (m *NullCollector) CollectMetadata(_ *Metadata) {}

func (m *NullCollector) CollectEvent(_ Event) {}

func (m *NullCollector) CollectEvents(ev Event, count uint64) {}

func (m *NullCollector) SetInstallationID(_ string) {}

func (m *NullCollector) CollectCommPrefs(_, _ string, _, _ bool) {}

func (m *NullCollector) Close() {}
