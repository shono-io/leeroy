package salesforce

type ReplayState interface {
	Get() []byte
	Set(replayId []byte) error
}

type MemoryReplayState struct {
	state []byte
}

func (m *MemoryReplayState) Get() []byte {
	return m.state
}

func (m *MemoryReplayState) Set(replayId []byte) error {
	m.state = replayId
	return nil
}
