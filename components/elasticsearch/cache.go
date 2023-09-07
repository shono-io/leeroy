package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/optype"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
	"github.com/sirupsen/logrus"
	"time"
)

func init() {
	err := service.RegisterCache("elasticsearch", cacheConfig(), newCache)
	if err != nil {
		logrus.Panicf("failed to register cache: %v", err)
	}
}

func cacheConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("index").Description("The Elasticsearch index to use for storing cache entries.")).
		Fields(clientFields()...)
}

func newCache(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
	cl, err := newClient(conf)
	if err != nil {
		return nil, err
	}

	idx, err := conf.FieldString("index")
	if err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

	return &cache{cl: cl, index: idx, logger: mgr.Logger()}, nil
}

type cache struct {
	cl     *elasticsearch.TypedClient
	index  string
	logger *service.Logger
}

func (c cache) Get(ctx context.Context, key string) ([]byte, error) {
	res, err := c.cl.Get(c.index, key).Do(ctx)
	if err != nil {
		return nil, err
	}
	if !res.Found {
		return nil, service.ErrKeyNotFound
	}

	return res.Source_, nil
}

func (c cache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	_, err := c.cl.Index(c.index).Id(key).Raw(bytes.NewBuffer(value)).Refresh(refresh.True).Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c cache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	_, err := c.cl.Index(c.index).
		Id(key).
		Raw(bytes.NewBuffer(value)).
		Refresh(refresh.True).
		OpType(optype.Create).
		Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c cache) Delete(ctx context.Context, key string) error {
	_, err := c.cl.Delete(c.index, key).Refresh(refresh.True).Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c cache) Close(ctx context.Context) error {
	return nil
}
