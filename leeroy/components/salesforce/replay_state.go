package salesforce

import (
	"context"
	"github.com/benthosdev/benthos/v4/public/service"
)

type ReplayState interface {
	Get(ctx context.Context) ([]byte, error)
	Set(ctx context.Context, replayId []byte) error
}

type MemoryReplayState struct {
	state []byte
}

func (m *MemoryReplayState) Get(ctx context.Context) ([]byte, error) {
	return m.state, nil
}

func (m *MemoryReplayState) Set(ctx context.Context, replayId []byte) error {
	m.state = replayId
	return nil
}

type CacheReplayState struct {
	mgr       *service.Resources
	cacheName string
	key       string
}

func (rs *CacheReplayState) Get(ctx context.Context) (result []byte, err error) {
	err = rs.mgr.AccessCache(ctx, rs.cacheName, func(c service.Cache) {
		result, err = c.Get(ctx, rs.key)
	})

	return result, err
}

func (rs *CacheReplayState) Set(ctx context.Context, replayId []byte) (err error) {
	return rs.mgr.AccessCache(ctx, rs.cacheName, func(c service.Cache) {
		err = c.Set(ctx, rs.key, replayId, nil)
	})
}
