package storage

import "context"

type Client interface {
	List(ctx context.Context, collection string, q string, paging *PagingOpts) (Cursor, error)
	Get(ctx context.Context, collection string, key string) (map[string]any, error)
	Set(ctx context.Context, collection string, key string, value map[string]any) error
	Merge(ctx context.Context, collection string, key string, value map[string]any) (map[string]any, error)
	Add(ctx context.Context, collection string, key string, value map[string]any) error
	Delete(ctx context.Context, collection string, key string) error
	Close() error
}

type PagingOpts struct {
	Offset int64
	Size   int64
}

type Cursor interface {
	HasNext() bool
	Read() (map[string]any, error)
	Close() error
}
