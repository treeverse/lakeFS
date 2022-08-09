package stats

type NullCollector struct{}

func (m *NullCollector) CollectMetadata(_ *Metadata) {}

func (m *NullCollector) CollectEvent(_, _ string) {}

func (m *NullCollector) SetInstallationID(_ string) {}

func (m *NullCollector) Close() {}
