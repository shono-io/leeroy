package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/sirupsen/logrus"
)

func init() {
	err := service.RegisterProcessor("storage", storeProcConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return procFromConfig(conf)
	})
	if err != nil {
		panic(err)
	}
}

func storeProcConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Integration")

	return spec.
		Field(service.NewObjectField("driver",
			service.NewObjectField("arangodb", ArangodbConfigFields()...).Default(nil),
			service.NewObjectField("elasticsearch", ElasticsearchConfigFields()...).Default(nil),
		)).
		Field(service.NewInterpolatedStringField("collection").
			Description("The reference to the concept to manipulate the store for")).
		Field(service.NewStringField("operation").
			Description("The operation to perform, one of: 'list', 'get', 'add', 'set', 'merge' or 'delete'")).
		Field(service.NewInterpolatedStringField("key").
			Description("The key to use. This is only applicable for 'get', 'add', 'set', 'merge' and 'delete'").
			Optional()).
		Field(service.NewInterpolatedStringField("q").
			Description("The query to pass to the driver. This is only applicable for 'list'").
			Optional())
}

func procFromConfig(conf *service.ParsedConfig) (proc *storeProc, err error) {
	proc = &storeProc{}

	collection, err := conf.FieldInterpolatedString("collection")
	if err != nil {
		return nil, fmt.Errorf("invalid collection: %w", err)
	}
	proc.collection = collection

	if conf.Contains("driver", "arangodb") {
		driver, err := NewArangodbClientFromConfig(conf.Namespace("driver", "arangodb"))
		if err != nil {
			return nil, fmt.Errorf("failed to create arangodb driver: %w", err)
		}
		proc.driver = driver
	} else if conf.Contains("driver", "elasticsearch") {
		driver, err := NewElasticsearchClientFromConfig(conf.Namespace("driver", "elasticsearch"))
		if err != nil {
			return nil, fmt.Errorf("failed to create elasticsearch driver: %w", err)
		}
		proc.driver = driver
	} else {
		return nil, fmt.Errorf("no driver specified")
	}

	proc.operation, err = conf.FieldString("operation")
	if err != nil {
		return nil, fmt.Errorf("failed to get operation: %w", err)
	}

	if conf.Contains("key") {
		proc.key, err = conf.FieldInterpolatedString("key")
		if err != nil {
			return nil, fmt.Errorf("failed to get key: %w", err)
		}
	}

	if conf.Contains("q") {
		proc.q, err = conf.FieldInterpolatedString("q")
		if err != nil {
			return nil, fmt.Errorf("failed to get query: %w", err)
		}
	}

	return proc, nil
}

type storeProc struct {
	driver     Client
	collection *service.InterpolatedString

	operation string
	key       *service.InterpolatedString
	q         *service.InterpolatedString
}

func (s *storeProc) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	switch s.operation {
	case "get":
		return s.processGet(ctx, message)
	case "add":
		return s.processAdd(ctx, message)
	case "set":
		return s.processReplace(ctx, message)
	case "delete":
		return s.processDelete(ctx, message)
	case "list":
		return s.processList(ctx, message)
	default:
		return nil, fmt.Errorf("unknown operation: %s", s.operation)
	}
}

func (s *storeProc) Close(ctx context.Context) error {
	return s.driver.Close()
}

func (s *storeProc) processGet(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	// -- get the key from the message
	key, err := s.key.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	col, err := s.collection.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("invalid collection: %w", err)
	}

	res, err := s.driver.Get(ctx, col, key)
	if err != nil {
		return nil, fmt.Errorf("unable to read document with key %q: %w", key, err)
	}

	result := service.NewMessage(nil)
	result.SetStructured(res)

	CopyMeta(message, result)

	return service.MessageBatch{result}, nil

}

func (s *storeProc) processAdd(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	// -- get the key from the message
	key, err := s.key.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	data, err := s.getMessagePayload(message)
	if err != nil {
		return nil, err
	}

	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		b, _ := json.Marshal(data)
		logrus.Tracef("adding document %q as %s", key, b)
	}

	col, err := s.collection.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("invalid collection: %w", err)
	}

	if err := s.driver.Add(ctx, col, key, data); err != nil {
		return nil, fmt.Errorf("unable to add document with key %s: %w", key, err)
	}

	result := service.NewMessage(nil)
	result.SetStructured(data)

	CopyMeta(message, result)

	return service.MessageBatch{result}, nil
}

func (s *storeProc) processReplace(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	// -- get the key from the message
	key, err := s.key.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	data, err := s.getMessagePayload(message)
	if err != nil {
		return nil, err
	}

	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		b, _ := json.Marshal(data)
		logrus.Tracef("setting document %q to %s", key, b)
	}

	col, err := s.collection.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("invalid collection: %w", err)
	}

	if err := s.driver.Set(ctx, col, key, data); err != nil {
		return nil, fmt.Errorf("unable to set document with key %s: %w", key, err)
	}

	result := service.NewMessage(nil)
	result.SetStructured(data)

	CopyMeta(message, result)

	return service.MessageBatch{result}, nil
}

func (s *storeProc) processMerge(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	// -- get the key from the message
	key, err := s.key.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	data, err := s.getMessagePayload(message)
	if err != nil {
		return nil, err
	}

	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		b, _ := json.Marshal(data)
		logrus.Tracef("setting document %q to %s", key, b)
	}

	col, err := s.collection.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("invalid collection: %w", err)
	}

	merged, err := s.driver.Merge(ctx, col, key, data)
	if err != nil {
		return nil, fmt.Errorf("unable to set document with key %s: %w", key, err)
	}

	result := service.NewMessage(nil)
	result.SetStructured(merged)

	CopyMeta(message, result)

	return service.MessageBatch{result}, nil
}

func (s *storeProc) processDelete(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	// -- get the key from the message
	key, err := s.key.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		logrus.Tracef("removing document %q", key)
	}

	col, err := s.collection.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("invalid collection: %w", err)
	}

	// -- get the document so we can return it
	data, err := s.driver.Get(ctx, col, key)
	if err != nil {
		return nil, fmt.Errorf("unable to read document with key %q: %w", key, err)
	}

	if err := s.driver.Delete(ctx, col, key); err != nil {
		return nil, fmt.Errorf("unable to delete document with key %s: %w", key, err)
	}

	result := service.NewMessage(nil)
	result.SetStructured(data)

	CopyMeta(message, result)

	return service.MessageBatch{result}, nil
}

func (s *storeProc) processList(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	q, err := s.q.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	col, err := s.collection.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("invalid collection: %w", err)
	}

	cur, err := s.driver.List(ctx, col, q, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list documents: %w", err)
	}

	var res []*service.Message
	for cur.HasNext() {
		doc, err := cur.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read document: %w", err)
		}

		result := service.NewMessage(nil)
		result.SetStructured(doc)

		CopyMeta(message, result)

		res = append(res, result)
	}

	return res, nil
}

func (s *storeProc) getMessagePayload(message *service.Message) (map[string]any, error) {
	//sd, err := s.value.Query(message)
	sd, err := message.AsStructuredMut()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the value: %w", err)
	}

	switch data := sd.(type) {
	case map[string]any:
		return data, nil
	case *service.Message:
		m, err := data.AsStructuredMut()
		if err != nil {
			return nil, fmt.Errorf("failed to get the value from the message: %w", err)
		}

		switch dt := m.(type) {
		case map[string]any:
			return dt, nil
		default:
			return nil, fmt.Errorf("unsupported mapped message payload type: %T", sd)
		}

	default:
		return nil, fmt.Errorf("unsupported message payload type: %T", sd)
	}
}

func CopyMeta(src, dst *service.Message) {
	_ = src.MetaWalk(func(k string, v string) error {
		dst.MetaSetMut(k, v)
		return nil
	})
}
