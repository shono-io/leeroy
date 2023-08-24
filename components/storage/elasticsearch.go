package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/eql/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/optype"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
)

func NewElasticsearchClientFromConfig(conf *service.ParsedConfig) (Client, error) {
	cfg, err := clientConfigFromConfig(conf)
	if err != nil {
		return nil, err
	}

	cl, err := elasticsearch.NewTypedClient(*cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create the elasticsearch client: %w", err)
	}

	return &ElasticsearchClient{cl: cl}, nil
}

func ElasticsearchConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("addresses").Description("A list of Elasticsearch addresses to connect to.").Default([]string{}),
		service.NewStringField("username").Description("The username to use for authentication.").Default(""),
		service.NewStringField("password").Description("The password to use for authentication.").Default(""),
		service.NewStringField("cloud_id").Description("The cloud ID to use for authentication.").Default(""),
		service.NewStringField("api_key").Description("The API key to use for authentication.").Default(""),
		service.NewStringField("service_token").Description("The service token to use for authentication.").Default(""),
		service.NewStringField("certificate_fingerprint").Description("The certificate fingerprint to use for authentication.").Default(""),
	}
}

func clientConfigFromConfig(conf *service.ParsedConfig) (*elasticsearch.Config, error) {
	result := &elasticsearch.Config{
		Addresses:              []string{},
		Username:               "",
		Password:               "",
		CloudID:                "",
		APIKey:                 "",
		ServiceToken:           "",
		CertificateFingerprint: "",
	}

	addresses, err := conf.FieldStringList("addresses")
	if err != nil {
		return nil, fmt.Errorf("failed to parse addresses: %w", err)
	}
	if len(addresses) > 0 {
		result.Addresses = addresses
	}

	username, err := conf.FieldString("username")
	if err != nil {
		return nil, fmt.Errorf("failed to parse username: %w", err)
	}
	result.Username = username

	password, err := conf.FieldString("password")
	if err != nil {
		return nil, fmt.Errorf("failed to parse password: %w", err)
	}
	result.Password = password

	cloudID, err := conf.FieldString("cloud_id")
	if err != nil {
		return nil, fmt.Errorf("failed to parse cloud_id: %w", err)
	}
	result.CloudID = cloudID

	apiKey, err := conf.FieldString("api_key")
	if err != nil {
		return nil, fmt.Errorf("failed to parse api_key: %w", err)
	}
	result.APIKey = apiKey

	serviceToken, err := conf.FieldString("service_token")
	if err != nil {
		return nil, fmt.Errorf("failed to parse service_token: %w", err)
	}
	result.ServiceToken = serviceToken

	certificateFingerprint, err := conf.FieldString("certificate_fingerprint")
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate_fingerprint: %w", err)
	}
	result.CertificateFingerprint = certificateFingerprint

	return result, nil
}

type ElasticsearchClient struct {
	cl *elasticsearch.TypedClient
}

func (c *ElasticsearchClient) List(ctx context.Context, collection string, q string, paging *PagingOpts) (Cursor, error) {
	panic("not implemented yet")
}

func (c *ElasticsearchClient) Get(ctx context.Context, collection string, key string) (map[string]any, error) {
	res, err := c.cl.Get(collection, key).Do(ctx)
	if err != nil {
		return nil, err
	}
	if !res.Found {
		return nil, service.ErrKeyNotFound
	}

	var result map[string]any
	if err := json.Unmarshal(res.Source_, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *ElasticsearchClient) Set(ctx context.Context, collection string, key string, value map[string]any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	_, err = c.cl.Index(collection).Id(key).Raw(bytes.NewBuffer(b)).Refresh(refresh.True).Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *ElasticsearchClient) Merge(ctx context.Context, collection string, key string, value map[string]any) (map[string]any, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	fnd, err := c.cl.Exists(collection, key).IsSuccess(ctx)
	if err != nil {
		return nil, err
	}

	if fnd {
		_, err = c.cl.Update(collection, key).
			Raw(bytes.NewBuffer(b)).
			Refresh(refresh.True).
			Do(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		if err = c.Add(ctx, collection, key, value); err != nil {
			return nil, err
		}
	}

	return c.Get(ctx, collection, key)
}

func (c *ElasticsearchClient) Add(ctx context.Context, collection string, key string, value map[string]any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	_, err = c.cl.Index(collection).
		Id(key).
		Raw(bytes.NewBuffer(b)).
		Refresh(refresh.True).
		OpType(optype.Create).
		Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *ElasticsearchClient) Delete(ctx context.Context, collection string, key string) error {
	_, err := c.cl.Delete(collection, key).Refresh(refresh.True).Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *ElasticsearchClient) Close() error {
	return nil
}

type elasticsearchCursor struct {
	response *search.Response
}

func (c *elasticsearchCursor) HasNext() bool {
	//TODO implement me
	panic("implement me")
}

func (c *elasticsearchCursor) Read() (map[string]any, error) {
	//TODO implement me
	panic("implement me")
}

func (c *elasticsearchCursor) Close() error {
	//TODO implement me
	panic("implement me")
}
