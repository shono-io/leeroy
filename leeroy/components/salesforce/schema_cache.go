package salesforce

import (
	"context"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"time"
)

type Schemas interface {
	Fetch(schemaId string) (*goavro.Codec, error)
}

func NewSchemas(psc PubSubClient, timeout time.Duration, authContext context.Context) Schemas {
	return &cachedSchemas{
		schemas: &clientSchemas{
			timeout:     timeout,
			psc:         psc,
			authContext: authContext,
		},
		schemaCache: make(map[string]*goavro.Codec),
	}
}

type cachedSchemas struct {
	schemaCache map[string]*goavro.Codec
	schemas     Schemas
}

func (c *cachedSchemas) Fetch(schemaId string) (*goavro.Codec, error) {
	codec, ok := c.schemaCache[schemaId]
	if ok {
		return codec, nil
	}

	codec, err := c.schemas.Fetch(schemaId)
	if err != nil {
		return nil, err
	}

	c.schemaCache[schemaId] = codec

	return codec, nil
}

type clientSchemas struct {
	timeout     time.Duration
	psc         PubSubClient
	authContext context.Context
}

func (c *clientSchemas) Fetch(schemaId string) (*goavro.Codec, error) {
	var trailer metadata.MD

	req := &SchemaRequest{
		SchemaId: schemaId,
	}

	ctx, cancelFn := context.WithTimeout(c.authContext, c.timeout)
	defer cancelFn()

	schema, err := c.psc.GetSchema(ctx, req, grpc.Trailer(&trailer))
	if err != nil {
		return nil, err
	}

	return goavro.NewCodec(schema.GetSchemaJson())
}
