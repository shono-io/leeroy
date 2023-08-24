package reactor

import (
	"context"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

func init() {
	logrus.Debugf("registering processor: %s", "fork")
	err := service.RegisterProcessor("fork", config(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newProcessor(conf, mgr)
	})
	if err != nil {
		logrus.Panicf("failed to register processor: %s", err)
	}
}

func config() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewProcessorListField("processors"))
}

func newProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	processors, err := conf.FieldProcessorList("processors")
	if err != nil {
		return nil, err
	}

	return &proc{
		processors: processors,
	}, nil
}

type proc struct {
	processors []*service.OwnedProcessor
}

func (p *proc) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	// -- execute the processors
	res, err := service.ExecuteProcessors(ctx, p.processors, []*service.Message{message})
	if err != nil {
		return nil, err
	}

	cnt := 0
	errCnt := 0
	var errs error
	for _, m := range res {
		for _, v := range m {
			if v != nil {
				cnt++
				if err := v.GetError(); err != nil {
					errs = multierr.Append(errs, err)
					errCnt++
				}
			}
		}
	}

	// -- return the result
	result := message.DeepCopy()
	result.SetStructuredMut(map[string]any{
		"count":       cnt,
		"error_count": errCnt,
	})
	if errs != nil {
		result.SetError(errs)
	}

	return service.MessageBatch{result}, nil
}

func (p *proc) Close(ctx context.Context) error {
	return nil
}
