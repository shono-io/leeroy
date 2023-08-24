package reactor

import (
	"context"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/shono-io/leeroy/core"
	"github.com/sirupsen/logrus"
	"strings"
)

func init() {
	logrus.Debugf("registering processor: %s", "reactor")
	err := service.RegisterProcessor("reactor", config(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newProcessor(conf, mgr)
	})
	if err != nil {
		logrus.Panicf("failed to register processor: %s", err)
	}
}

func config() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("namespace").Default("")).
		Field(service.NewObjectListField("events",
			service.NewObjectField("on",

				service.NewStringField("scope"),
				service.NewStringField("concept"),
				service.NewStringField("event"),
			),
			service.NewProcessorListField("processors"),
		))
}

func newProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	events, err := conf.FieldObjectList("events")
	if err != nil {
		return nil, err
	}

	namespace, err := conf.FieldString("namespace")
	if err != nil {
		return nil, err
	}

	handlers := map[eventHeader]handler{}

	for _, event := range events {
		trigger, err := eventHeaderFromConfig(event, "on")
		if err != nil {
			return nil, fmt.Errorf("failed to parse trigger: %w", err)
		}

		processors, err := event.FieldProcessorList("processors")
		if err != nil {
			return nil, err
		}

		handlers[*trigger] = handler{
			trigger:    *trigger,
			processors: processors,
		}
	}

	return &proc{
		namespace: namespace,
		handlers:  handlers,
	}, nil
}

func eventHeaderFromConfig(conf *service.ParsedConfig, path ...string) (*eventHeader, error) {
	scope, err := conf.FieldString(append(path, "scope")...)
	if err != nil {
		return nil, err
	}

	concept, err := conf.FieldString(append(path, "concept")...)
	if err != nil {
		return nil, err
	}

	event, err := conf.FieldString(append(path, "event")...)
	if err != nil {
		return nil, err
	}

	return &eventHeader{
		scope:   scope,
		concept: concept,
		event:   event,
	}, nil
}

func eventHeaderFromMessage(namespace string, message *service.Message) *eventHeader {
	prefix := namespace
	if prefix != "" && !strings.HasSuffix(prefix, ".") {
		prefix += "."
	}

	scope, _ := message.MetaGet(prefix + "scope")
	concept, _ := message.MetaGet(prefix + "concept")
	event, _ := message.MetaGet(prefix + "event")

	if scope == "" || concept == "" || event == "" {
		return nil
	}

	return &eventHeader{
		scope:   scope,
		concept: concept,
		event:   event,
	}
}

type eventHeader struct {
	scope   string
	concept string
	event   string
}

type handler struct {
	trigger    eventHeader
	processors []*service.OwnedProcessor
}

type proc struct {
	namespace string
	handlers  map[eventHeader]handler
}

func (p *proc) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	eh := eventHeaderFromMessage(p.namespace, message)
	if eh == nil {
		return nil, fmt.Errorf("event headers missing")
	}

	// -- lookup for the event ref in the handlers
	h, fnd := p.handlers[*eh]
	if !fnd {
		// -- skip the message if no handlers are found
		return nil, nil
	}

	// -- add the concept reference to the message context
	message = message.WithContext(context.WithValue(message.Context(), "concept_ref", core.NewConceptReference(eh.scope, eh.concept)))

	// -- execute the processors
	res, err := service.ExecuteProcessors(ctx, h.processors, []*service.Message{message})
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	return res[0], nil
}

func (p *proc) Close(ctx context.Context) error {
	return nil
}
