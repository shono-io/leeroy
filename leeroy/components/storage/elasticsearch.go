package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/optype"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
)

func IsElasticsearchConfigured(conf *service.ParsedConfig) bool {
	_, err := conf.FieldStringList("elasticsearch", "addresses")
	hasAddresses := err == nil

	_, err = conf.FieldString("elasticsearch", "cloud_id")
	hasCloudId := err == nil

	return hasAddresses || hasCloudId
}

func NewElasticsearchClientFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (Client, error) {
	cfg, err := clientConfigFromConfig(conf)
	if err != nil {
		return nil, err
	}

	cl, err := elasticsearch.NewTypedClient(*cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create the elasticsearch client: %w", err)
	}

	return &ElasticsearchClient{cl: cl, logger: mgr.Logger()}, nil
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
	cl     *elasticsearch.TypedClient
	logger *service.Logger
}

func (c *ElasticsearchClient) ParseQuery(config string) (any, error) {
	var subQry types.Query
	if err := json.Unmarshal([]byte(config), &subQry); err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}
	return &subQry, nil
}

func (c *ElasticsearchClient) List(ctx context.Context, collection string, q any, pitEnabled bool, paging *PagingOpts) (Cursor, error) {
	if q == nil {
		return nil, fmt.Errorf("query is nil")
	}

	if pitEnabled {
		qry, ok := q.(*types.Query)
		if !ok {
			return nil, fmt.Errorf("query is not a valid elasticsearch query; a pit query requires only the query part of a full search message")
		}

		resp, err := c.cl.OpenPointInTime(collection).KeepAlive("1m").Do(ctx)
		if err != nil {
			return nil, err
		}

		pit := &types.PointInTimeReference{
			Id:        resp.Id,
			KeepAlive: "1m",
		}

		sort := types.NewSortOptions()
		sort.Doc_ = &types.ScoreSort{Order: &sortorder.Desc}

		if c.logger != nil {
			b, _ := json.Marshal(qry)
			c.logger.Debugf("executing %s", string(b))
		}

		req := c.cl.Search().Query(qry).Pit(pit).Sort(sort).Size(100).TrackScores(true)

		return newEsCursor(ctx, c.cl, req, pit)
	} else {
		qry, ok := q.(string)
		if !ok {
			return nil, fmt.Errorf("a full search message is required when pits are disabled")
		}

		req := c.cl.Search().Raw(bytes.NewReader([]byte(qry)))
		return newEsCursor(ctx, c.cl, req, nil)
	}
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
	_, hasDoc := value["doc"]
	_, hasScript := value["script"]

	if !hasDoc && !hasScript {
		value = map[string]any{
			"doc": value,
		}
	}

	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	fnd, err := c.cl.Exists(collection, key).IsSuccess(ctx)
	if err != nil {
		return nil, err
	}

	if fnd {
		_, err := c.cl.Update(collection, key).
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

func newEsCursor(ctx context.Context, cl *elasticsearch.TypedClient, req *search.Search, pit *types.PointInTimeReference) (Cursor, error) {
	searchResp, err := req.Do(ctx)
	if err != nil {
		return nil, err
	}

	if pit == nil {
		return &singlePageEsCursor{
			ctx:        ctx,
			pageOffset: 0,
			hits:       searchResp.Hits.Hits,
		}, nil
	} else {
		return &pagingEsCursor{
			ctx:        ctx,
			cl:         cl,
			req:        req,
			pit:        pit,
			pageOffset: 0,
			hits:       searchResp.Hits.Hits,
		}, nil
	}
}

type singlePageEsCursor struct {
	ctx        context.Context
	pageOffset int
	hits       []types.Hit
}

func (s *singlePageEsCursor) HasNext() bool {
	return s.pageOffset < len(s.hits)
}

func (s *singlePageEsCursor) Read() (map[string]any, error) {
	hit := s.hits[s.pageOffset]

	var result map[string]any
	if err := json.Unmarshal(hit.Source_, &result); err != nil {
		return nil, err
	}

	result["_score"] = float64(hit.Score_)
	s.pageOffset++

	return result, nil
}

func (s *singlePageEsCursor) Close() error {
	return nil
}

type pagingEsCursor struct {
	ctx context.Context
	cl  *elasticsearch.TypedClient
	pit *types.PointInTimeReference

	// -- the offset within the current batch, reset to 0 when a new batch is fetched
	pageOffset int64

	// -- the current batch of results
	hits []types.Hit

	req      *search.Search
	lastSort []types.FieldValue
}

func (c *pagingEsCursor) HasNext() bool {
	return len(c.hits) > 0
}

func (c *pagingEsCursor) Read() (map[string]any, error) {
	hit := c.hits[c.pageOffset]

	var result map[string]any
	if err := json.Unmarshal(hit.Source_, &result); err != nil {
		return nil, err
	}

	result["_score"] = float64(hit.Score_)

	c.lastSort = hit.Sort
	c.pageOffset++

	if c.pageOffset >= int64(len(c.hits)) {
		if err := c.loadMore(); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (c *pagingEsCursor) Close() error {
	_, err := c.cl.ClosePointInTime().Id(c.pit.Id).Do(c.ctx)
	return err
}

func (c *pagingEsCursor) loadMore() error {
	req := c.req.SearchAfter(c.lastSort...)

	resp, err := req.Do(c.ctx)
	if err != nil {
		return err
	}

	c.hits = resp.Hits.Hits
	c.pageOffset = 0

	return nil
}
