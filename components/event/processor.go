package event

import (
	"context"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/sirupsen/logrus"
	"strings"
)

func init() {
	logrus.Debugf("registering processor: %s", "event")
	err := service.RegisterProcessor("event", config(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newProcessor(conf, mgr)
	})
	if err != nil {
		logrus.Panicf("failed to register processor: %s", err)
	}
}

func config() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("namespace").Default("io.shono")).
		Field(service.NewInterpolatedStringField("scope")).
		Field(service.NewInterpolatedStringField("concept")).
		Field(service.NewInterpolatedStringField("event")).
		Field(service.NewInterpolatedStringField("key"))
}

func newProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	namespace, err := conf.FieldString("namespace")
	if err != nil {
		return nil, err
	}

	if namespace != "" && !strings.HasSuffix(namespace, ".") {
		namespace += "."
	}

	scope, err := conf.FieldInterpolatedString(fmt.Sprintf("scope"))
	if err != nil {
		return nil, err
	}

	concept, err := conf.FieldInterpolatedString("concept")
	if err != nil {
		return nil, err
	}

	event, err := conf.FieldInterpolatedString("event")
	if err != nil {
		return nil, err
	}

	key, err := conf.FieldInterpolatedString("key")
	if err != nil {
		return nil, err
	}

	return &proc{
		namespace:   namespace,
		scopeExpr:   scope,
		conceptExpr: concept,
		eventExpr:   event,
		keyExpr:     key,
	}, nil
}

type proc struct {
	namespace   string
	scopeExpr   *service.InterpolatedString
	conceptExpr *service.InterpolatedString
	eventExpr   *service.InterpolatedString
	keyExpr     *service.InterpolatedString
}

func (p *proc) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	// -- take a copy of the original message
	result := message.Copy()

	// -- add the headers
	scope, err := p.scopeExpr.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to parse scope: %w", err)
	}
	result.MetaSetMut(p.namespace+"scope", scope)

	concept, err := p.conceptExpr.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to parse concept: %w", err)
	}
	result.MetaSetMut(p.namespace+"concept", concept)

	event, err := p.eventExpr.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to parse event: %w", err)
	}
	result.MetaSetMut(p.namespace+"event", event)

	key, err := p.keyExpr.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key: %w", err)
	}
	result.MetaSetMut(p.namespace+"key", key)

	return []*service.Message{result}, nil
}

func (p *proc) Close(ctx context.Context) error {
	return nil
}
