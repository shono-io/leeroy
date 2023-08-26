package storage

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/benthosdev/benthos/v4/public/service"
)

func IsArangodbConfigured(conf *service.ParsedConfig) bool {
	_, err := conf.FieldStringList("arangodb", "urls")
	return err == nil
}

func ArangodbConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("urls"),
		service.NewStringField("username"),
		service.NewStringField("password"),
		service.NewStringField("database"),
	}
}

func NewArangodbClientFromConfig(conf *service.ParsedConfig) (Client, error) {
	urls, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, fmt.Errorf("failed to get urls field: %w", err)
	}

	username, err := conf.FieldString("username")
	if err != nil {
		return nil, fmt.Errorf("failed to get username field: %w", err)
	}

	password, err := conf.FieldString("password")
	if err != nil {
		return nil, fmt.Errorf("failed to get password field: %w", err)
	}

	database, err := conf.FieldString("database")
	if err != nil {
		return nil, fmt.Errorf("failed to get database field: %w", err)
	}

	var u []string
	for _, v := range urls {
		u = append(u, v)
	}

	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: u,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create arangodb connection: %w", err)
	}
	c, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(username, password),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create arangodb client: %w", err)
	}

	db, err := c.Database(context.Background(), database)
	if err != nil {
		return nil, fmt.Errorf("failed to get arangodb database: %w", err)
	}

	return ArangodbClient{c, db}, nil
}

type ArangodbClient struct {
	cl driver.Client
	db driver.Database
}

func (c ArangodbClient) Merge(ctx context.Context, collection string, key string, value map[string]any) (map[string]any, error) {
	panic("not supported yet")
}

func (c ArangodbClient) List(ctx context.Context, collection string, q string, paging *PagingOpts) (Cursor, error) {
	q, err := c.buildQuery(collection, q, paging)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	cursor, err := c.db.Query(ctx, q, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return &arangodbCursorWrapper{cursor, ctx}, nil
}

func (c ArangodbClient) Get(ctx context.Context, collection string, key string) (map[string]any, error) {
	col, err := c.db.Collection(ctx, collection)
	if err != nil {
		if driver.IsNotFoundGeneral(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get collection: %w", err)
	}

	var target map[string]any
	_, err = col.ReadDocument(ctx, key, &target)
	return target, err
}

func (c ArangodbClient) Set(ctx context.Context, collection string, key string, value map[string]any) error {
	col, err := c.db.Collection(ctx, collection)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	fnd, err := col.DocumentExists(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to check if document exists: %w", err)
	}

	// -- override the key
	value["_key"] = key

	if fnd {
		_, err = col.ReplaceDocument(ctx, key, value)
		return err
	} else {
		_, err = col.CreateDocument(ctx, value)
		return err
	}
}

func (c ArangodbClient) Add(ctx context.Context, collection string, key string, value map[string]any) error {
	col, err := c.db.Collection(ctx, collection)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	// -- override the key
	value["_key"] = key

	_, err = col.CreateDocument(ctx, value)
	return err
}

func (c ArangodbClient) Delete(ctx context.Context, collection string, key string) error {
	col, err := c.db.Collection(ctx, collection)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	_, err = col.RemoveDocument(ctx, key)
	return err
}

func (c ArangodbClient) Close() error {
	return nil
}

func (c ArangodbClient) buildQuery(collection string, q string, paging *PagingOpts) (string, error) {
	result := fmt.Sprintf("FOR d IN %s", collection)

	var parts []string

	if len(q) > 0 {
		parts = append(parts, fmt.Sprintf("FILTER %s", q))
	}

	if paging != nil {
		parts = append(parts, fmt.Sprintf("LIMIT %d, %d", paging.Offset, paging.Size))
	}

	result += " RETURN d"

	return result, nil
}

type arangodbCursorWrapper struct {
	c   driver.Cursor
	ctx context.Context
}

func (c *arangodbCursorWrapper) Close() error {
	return c.c.Close()
}

func (c *arangodbCursorWrapper) Count() int64 {
	return c.c.Count()
}

func (c *arangodbCursorWrapper) HasNext() bool {
	return c.c.HasMore()
}

func (c *arangodbCursorWrapper) Read() (map[string]any, error) {
	var target map[string]any
	_, err := c.c.ReadDocument(c.ctx, target)
	return target, err
}
