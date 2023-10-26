package stats

type NullCollector struct{}

func (m *NullCollector) CollectMetadata(_ *Metadata) {}

func (m *NullCollector) CollectEvent(_ Event) {}

func (m *NullCollector) CollectEvents(_ Event, _ uint64) {}

func (m *NullCollector) SetInstallationID(_ string) {}

func (m *NullCollector) CollectCommPrefs(_ CommPrefs) {}

func (m *NullCollector) Close() {}
